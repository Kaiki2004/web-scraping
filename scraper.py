"""
SCRAPE -> NORMALIZAÇÃO -> MYSQL
- Lê uma planilha com nomes/URLs de produtos (produtos.xlsx)
- Faz web scraping (Magalu / KaBuM! / genérico) para preço, vendedor, avaliação e frete
- Salva resultados em Excel
- Normaliza os campos e insere no MySQL no esquema solicitado (Fornecedores, Seller, Products, List)
"""

import os, re, time, random, sys, json, math, hashlib, warnings, argparse
from dataclasses import dataclass
from typing import Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

# Selenium (para o scraping)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, JavascriptException, InvalidSessionIdException, WebDriverException
from selenium.webdriver.chrome.service import Service
try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WDM = True
except Exception:
    USE_WDM = False

# SQLAlchemy (para inserir no MySQL)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =========================
# CONFIG (env com fallback)
# =========================
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "admin")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DB   = os.environ.get("MYSQL_DB", "ecommerce_scraping")

CEP_DEFAULT = "14401-426"  # usado para cálculo de frete quando possível

# =========================
# REGEX e seletores básicos
# =========================
CURRENCY_RE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}")
NUMBER_RE   = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}")
RATING_RE   = re.compile(r"(\d+[.,]\d+)\s*(?:/|de)?\s*5")
COUNT_RE    = re.compile(r"(\d{1,3}(?:\.\d{3})*)")

SELECTORS_PRICE_MAGALU = ['[data-testid="price-value"]','[data-testid="price-big"]','[data-testid="price-amount"]','[class*="Price"]','[itemprop="price"]']
SELECTORS_PRICE_KABUM  = ['[data-testid="product-price"]','span.finalPrice','h4.finalPrice','.priceCard strong','[itemprop="price"]']
SELECTORS_PRICE_COMMON = ['[itemprop="price"]','[data-testid*="price"]','[class*="price"]']

SELECTORS_CEP_INPUT = [
    'input[name*="cep"]','input[id*="cep"]','input[placeholder*="CEP"]','input[aria-label*="CEP"]',
    'input[name*="zip"]','input[id*="zip"]','input[placeholder*="Código postal"]','input[aria-label*="zip"]'
]

# =========================
# Funções utilitárias (ETL)
# =========================
def slugify(s: str, max_len: int = 100) -> str:
    """Gera um 'code' curto, minúsculo e sem acentos para chaves únicas."""
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s).strip("-")
    return s[:max_len]

def parse_price(value) -> Optional[float]:
    """Converte diversos formatos de preço BR em float (ou None)."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    s = str(value).strip().replace("R$", "").replace(".", "").replace(" ", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except:
        return None

def parse_avaliacao(value) -> Optional[float]:
    """Extrai nota 0..5 de strings como '4,5 de 5' ou '4.5/5'. Se inválida, retorna None."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip().lower()

    # contagem de avaliações (ex.: '27 avaliações') não é nota
    if "avaliaç" in s and ("de 5" not in s and "/5" not in s):
        return None

    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:de|/)\s*5\b", s)
    if m:
        try:
            v = float(m.group(1).replace(",", "."))
            return round(v, 2) if 0 <= v <= 5 else None
        except:
            return None

    m = re.search(r"\b(\d+(?:[.,]\d+)?)\b", s)
    if m:
        try:
            v = float(m.group(1).replace(",", "."))
            return round(v, 2) if 0 <= v <= 5 else None
        except:
            return None
    return None

BRANDS = ["Apple","Samsung","Motorola","Xiaomi","Nokia","Asus","Google","Sony","LG","Realme","OnePlus","Huawei","Infinix","OPPO","Vivo","Lenovo"]

def extract_product_fields(produto: str) -> Tuple[str, str, str]:
    """Heurística simples para marca / modelo / variante (armazenamento/cor)."""
    if not produto:
        return ("Desconhecida", "", "")
    brand = "Desconhecida"
    p = produto.strip()

    found = None
    for b in BRANDS:
        if re.search(rf"\b{re.escape(b)}\b", p, flags=re.IGNORECASE):
            found = b; break
    if found: brand = found

    variant_parts = []
    storage = re.findall(r"\b(\d{2,4}\s?GB)\b", p, flags=re.IGNORECASE)
    if storage: variant_parts.append(storage[0].upper())
    color = re.findall(r"\b(preto|black|azul|blue|verde|green|branco|white|cinza|gray|graphite|violet|violeta|pink|rosa)\b", p, flags=re.IGNORECASE)
    if color: variant_parts.append(color[0].capitalize())
    variant = " ".join(dict.fromkeys(variant_parts)) if variant_parts else ""

    model = p
    if found:
        model = re.split(rf"\b{re.escape(found)}\b", p, flags=re.IGNORECASE, maxsplit=1)[-1].strip()
    model = re.split(r"[-|,(]", model)[0].strip()
    if not model or len(model) < 3: model = p
    return (brand[:255], model[:255], variant[:255])

# =========================
# Normalização (para inserir)
# =========================
def normalize_scrape_df(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas do scraping e gera colunas finais."""
    rename_map = {
        "produto":"produto","url":"url","fornecedor":"fornecedor","fonte_coluna":"fonte_coluna",
        "seller":"seller","preco_num":"price_num","preco":"preco_texto","avaliacao":"avaliacao",
        "avaliacoes_qtd":"avaliacoes_qtd","frete_valor":"frete_price","frete_prazo":"prazo_entrega",
        "frete_metodo":"frete_metodo","data_coleta":"created_at",
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    df = df.rename(columns=cols)

    # preço
    if "price_num" in df.columns:
        df["price"] = df["price_num"].apply(parse_price)
    else:
        df["price"] = df.get("preco_texto", pd.Series([None]*len(df))).apply(parse_price)

    # avaliação (0..5)
    df["avaliacao_val"] = df.get("avaliacao", pd.Series([None]*len(df))).apply(parse_avaliacao)

    # frete
    df["frete_price_val"] = df.get("frete_price", pd.Series([None]*len(df))).apply(parse_price)

    # fornecedor/seller
    df["fornecedor_name"] = df.get("fornecedor") if "fornecedor" in df.columns else df.get("fonte_coluna")
    df["seller_name"]      = df.get("seller") if "seller" in df.columns else df["fornecedor_name"]

    # data de coleta
    if "created_at" in df.columns:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df["created_at_ts"] = pd.to_datetime(df["created_at"], errors="coerce")
    else:
        df["created_at_ts"] = pd.Timestamp.utcnow()

    keep = ["url","fornecedor_name","seller_name","produto","price","avaliacao_val","frete_price_val","prazo_entrega","created_at_ts"]
    for k in keep:
        if k not in df.columns: df[k] = None
    out = df[keep].copy()
    out = out.dropna(subset=["produto","fornecedor_name"], how="any")
    return out

# =========================
# DB Helpers (MySQL)
# =========================
def get_engine() -> Engine:
    """Cria engine SQLAlchemy para o MySQL."""
    uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

def get_or_create_fornecedor(engine: Engine, name: str) -> int:
    """Upsert simples de fornecedor (usa code=slug como chave)."""
    code = slugify(name or "desconhecido")
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id_fornecedor FROM Fornecedores WHERE code=:code"), {"code": code}).fetchone()
        if row: return row[0]
        conn.execute(text("INSERT INTO Fornecedores (name, code) VALUES (:name, :code)"),
                     {"name": (name or "Desconhecido")[:255], "code": code[:100]})
        row = conn.execute(text("SELECT id_fornecedor FROM Fornecedores WHERE code=:code"), {"code": code}).fetchone()
        return row[0]

def get_or_create_seller(engine: Engine, name: str, fk_fornecedor: int) -> int:
    """Upsert simples de seller por par (name, fornecedor)."""
    with engine.begin() as conn:
        row = conn.execute(text("""SELECT id_seller FROM Seller WHERE name=:n AND fk_fornecedor=:f"""),
                           {"n": (name or "Desconhecido")[:255], "f": fk_fornecedor}).fetchone()
        if row: return row[0]
        conn.execute(text("INSERT INTO Seller (name, fk_fornecedor) VALUES (:n, :f)"),
                     {"n": (name or "Desconhecido")[:255], "f": fk_fornecedor})
        row = conn.execute(text("""SELECT id_seller FROM Seller WHERE name=:n AND fk_fornecedor=:f"""),
                           {"n": (name or "Desconhecido")[:255], "f": fk_fornecedor}).fetchone()
        return row[0]

def get_or_create_product(engine: Engine, produto: str) -> int:
    """Upsert simples de produto por code (hash do nome normalizado)."""
    brand, model, variant = extract_product_fields(produto or "")
    norm = re.sub(r"\s+", " ", (produto or "").strip().lower())
    code_raw = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:20]
    code = f"p_{code_raw}"
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id_product FROM Products WHERE code=:code"), {"code": code}).fetchone()
        if row: return row[0]
        conn.execute(text("""INSERT INTO Products (brand, code, model, variante) 
                             VALUES (:b,:c,:m,:v)"""),
                     {"b": brand[:255], "c": code[:100], "m": model[:255], "v": variant[:255]})
        row = conn.execute(text("SELECT id_product FROM Products WHERE code=:code"), {"code": code}).fetchone()
        return row[0]

def insert_list_row(engine: Engine, url: Optional[str], price: Optional[float], avaliacao: Optional[float],
                    frete_price: Optional[float], prazo_entrega: Optional[str], created_at, 
                    fk_product: int, fk_seller: int, fk_fornecedor: int) -> None:
    """Insere uma linha em List (FKs já resolvidas)."""
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO List 
            (url, price, avaliacao, frete_price, prazo_entrega, created_at, fk_product, fk_seller, fk_fornecedor)
            VALUES (:url, :price, :aval, :frete, :prazo, :created, :p, :s, :f)"""),
            {"url": (url or "")[:500],
             "price": price if price is not None else None,
             "aval":  avaliacao if (avaliacao is not None and 0 <= avaliacao <= 5) else None,
             "frete": frete_price if frete_price is not None else None,
             "prazo": (prazo_entrega or "")[:100] if prazo_entrega else None,
             "created": pd.to_datetime(created_at) if created_at is not None else None,
             "p": fk_product, "s": fk_seller, "f": fk_fornecedor})

def ingest_dataframe(df: pd.DataFrame):
    """Normaliza o DataFrame de scraping e insere no MySQL."""
    norm = normalize_scrape_df(df)
    eng = get_engine()
    ins_ok = errs = 0
    for _, row in norm.iterrows():
        try:
            fk_fornecedor = get_or_create_fornecedor(eng, str(row["fornecedor_name"]).strip())
            fk_seller     = get_or_create_seller(eng, str(row["seller_name"]).strip() if row["seller_name"] else str(row["fornecedor_name"]).strip(), fk_fornecedor)
            fk_product    = get_or_create_product(eng, str(row["produto"]).strip())
            insert_list_row(eng,
                            url=row["url"] if pd.notna(row["url"]) else None,
                            price=row["price"] if pd.notna(row["price"]) else None,
                            avaliacao=(row["avaliacao_val"] if pd.notna(row["avaliacao_val"]) else None),
                            frete_price=(row["frete_price_val"] if pd.notna(row["frete_price_val"]) else None),
                            prazo_entrega=(row["prazo_entrega"] if pd.notna(row["prazo_entrega"]) else None),
                            created_at=(row["created_at_ts"] if pd.notna(row["created_at_ts"]) else None),
                            fk_product=fk_product, fk_seller=fk_seller, fk_fornecedor=fk_fornecedor)
            ins_ok += 1
        except Exception as e:
            errs += 1
            print(f"[ERRO][ingest] {type(e).__name__}: {e}")
    print(f"[INGEST] Inseridos: {ins_ok} | Erros: {errs}")

# =========================
# SCRAPING (SIMPLES e ROBUSTO)
# =========================
def clean_text(s): return re.sub(r"\s+", " ", str(s or "")).strip()

def br_to_float(txt):
    m = NUMBER_RE.search(txt or "")
    if not m: return None
    num = m.group(0).replace('.', '').replace(',', '.')
    try: return float(num)
    except: return None

def norm_price_str(txt):
    m = NUMBER_RE.search(txt or "")
    return f"R$ {m.group(0)}" if m else ""

def extract_jsonld_all(soup):
    vals = []
    for tag in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(tag.string or '')
        except Exception:
            continue
        if isinstance(data, list): vals.extend(data)
        else: vals.append(data)
    return vals

def jsonld_prices(soup):
    """Extrai possíveis preços de JSON-LD (quando sites expõem schema.org)."""
    out = []
    for obj in extract_jsonld_all(soup):
        try:
            offers = obj.get('offers') if isinstance(obj, dict) else None
            if not offers: continue
            if isinstance(offers, list):
                iters = offers
            else:
                iters = [offers]
            for o in iters:
                for key in ('price','lowPrice','highPrice'):
                    v = o.get(key) if isinstance(o, dict) else None
                    if isinstance(v,(int,float,str)): out.append(str(v))
        except Exception:
            pass
    normed = []
    for v in out:
        if ',' in str(v) and NUMBER_RE.search(str(v)):
            normed.append(norm_price_str(str(v)))
        else:
            try:
                f = float(v)
                br = f"R$ {f:,.2f}".replace(',', 'X').replace('.', ',').replace('X','.')
                normed.append(br)
            except Exception:
                pass
    return normed

def collect_dom_prices(soup, domain_hint=''):
    """Coleta candidatos a preço via CSS (prioriza seletores por domínio)."""
    sel_list = []
    if 'magalu' in domain_hint or 'magazineluiza' in domain_hint: sel_list.extend(SELECTORS_PRICE_MAGALU)
    if 'kabum' in domain_hint: sel_list.extend(SELECTORS_PRICE_KABUM)
    sel_list.extend(SELECTORS_PRICE_COMMON)

    cands = []
    for sel in sel_list:
        for el in soup.select(sel):
            txt = clean_text(el.get_text(' '))
            if not txt: continue
            if not (CURRENCY_RE.search(txt) or NUMBER_RE.search(txt)): continue
            cands.append((txt, f'sel:{sel}'))
    return cands

def pick_best_price(cands):
    """Escolhe o melhor candidato por menor valor e presença de 'R$'."""
    normed = []
    for txt, reason in cands:
        price_str = norm_price_str(txt); price_num = br_to_float(price_str)
        if price_str and price_num is not None:
            normed.append((price_str, price_num, reason))
    if not normed: return '', None, []
    normed.sort(key=lambda x: (x[1], x[2]))
    best = normed[0]
    debug = [f"{p} | {v} | {r}" for (p, v, r) in normed[:5]]
    return best[0], best[1], debug

def extract_jsonld_rating_and_count(soup):
    """Tenta capturar nota média e quantidade de avaliações do JSON-LD."""
    rating_val = ''
    count_val = None
    for obj in extract_jsonld_all(soup):
        try:
            ar = obj.get('aggregateRating') if isinstance(obj, dict) else None
            if isinstance(ar, dict):
                if ar.get('ratingValue') and not rating_val:
                    rating_val = str(ar['ratingValue'])
                cnt = ar.get('reviewCount') or ar.get('ratingCount')
                if cnt is not None and count_val is None:
                    if isinstance(cnt, (int, float)):
                        count_val = int(cnt)
                    else:
                        m = COUNT_RE.search(str(cnt))
                        if m: count_val = int(m.group(1).replace('.', ''))
        except Exception:
            pass
    return rating_val, count_val

def normalize_rating(r: str) -> str:
    """Formata nota em 'X,X de 5' quando possível."""
    r = clean_text(r)
    m = re.search(r"^\d+[.,]\d+$", r or '')
    if m:
        val = m.group(0).replace('.', ','); return f"{val} de 5"
    m2 = RATING_RE.search(r or '')
    if m2:
        val = m2.group(1).replace('.', ','); return f"{val} de 5"
    return r or ''

# ------ Selenium helpers ------
def _build_options(headless=True):
    opts = Options()
    if headless: opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox'); opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu'); opts.add_argument('--window-size=1366,900')
    opts.add_argument('--lang=pt-BR')
    opts.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
    return opts

def new_driver(headless=True):
    opts = _build_options(headless=headless)
    if USE_WDM:
        try:
            return webdriver.Chrome(options=opts)
        except Exception:
            return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return webdriver.Chrome(options=opts)

def ensure_driver(driver, headless=True):
    try:
        if driver is None: return new_driver(headless=headless)
        _ = driver.current_url
        return driver
    except Exception:
        try:
            if driver: driver.quit()
        except Exception: pass
        return new_driver(headless=headless)

def scrape_one(url: str, driver, cep: Optional[str] = None):
    """Coleta dados principais de uma página de produto."""
    started = datetime.now()
    status = 'ok'; erro = ''
    price = ''; price_num = None; price_debug = []
    seller = ''; rating = ''; count = None
    frete_valor = ''; frete_prazo = ''; frete_metodo = ''

    try:
        driver.get(url); time.sleep(0.5)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # preço (JSON-LD + seletores)
        prices = jsonld_prices(soup)
        cands = [(p, 'jsonld') for p in prices]
        netloc = urlparse(url).netloc.lower()
        hint = 'magalu' if 'magalu' in netloc or 'magazineluiza' in netloc else ('kabum' if 'kabum' in netloc else '')
        cands += collect_dom_prices(soup, hint)
        price, price_num, price_debug = pick_best_price(cands)

        # vendedor (quando disponível no JSON-LD)
        for obj in extract_jsonld_all(soup):
            try:
                offers = obj.get('offers') if isinstance(obj, dict) else None
                if isinstance(offers, dict) and isinstance(offers.get('seller'), dict):
                    seller = offers['seller'].get('name') or seller
            except Exception: pass
        if not seller:
            # fallback básico: procura por 'Vendido por ...'
            m = re.search(r'Vendido(?: e entregue)? por[: ]+([A-Za-z0-9\-\._\s]+)', html, flags=re.I)
            if m: seller = clean_text(m.group(1))
        if not seller:
            seller = 'Magalu' if 'magalu' in netloc else ('KaBuM!' if 'kabum' in netloc else '')

        # avaliação e contagem
        r_json, c_json = extract_jsonld_rating_and_count(soup)
        rating = normalize_rating(r_json) if r_json else rating
        count = c_json

        # frete (muitos sites exigem interação, aqui mantemos simples)
        # -> opcionalmente você pode interagir com o CEP e abrir modal, se necessário.

    except Exception as e:
        status = 'erro'; erro = f'{type(e).__name__}: {e}'
    finished = datetime.now()

    return {
        'preco': price, 'preco_num': price_num, 'fornecedor': seller,
        'avaliacao': rating, 'avaliacoes_qtd': count,
        'frete_valor': frete_valor, 'frete_prazo': frete_prazo, 'frete_metodo': frete_metodo,
        'data_coleta': finished.strftime('%Y-%m-%d %H:%M:%S'),
        'duracao_s': round((finished-started).total_seconds(), 2),
        'status': status, 'erro': erro, 'preco_debug': '; '.join(price_debug[:10]),
    }

def normalize_input_dataframe(df):
    """Detecta colunas de nome e de URL na planilha de entrada."""
    cols = list(df.columns)
    name_col = None
    for c in cols:
        if str(c).strip().lower() in {'nome','produto','product','item','descricao','descrição'}:
            name_col = c; break
    if name_col is None: name_col = cols[0]

    link_cols = [c for c in cols if 'link' in str(c).lower() or 'url' in str(c).lower()]
    if not link_cols:
        for c in cols:
            try:
                if df[c].astype(str).str.contains(r'https?://', na=False).any():
                    link_cols.append(c)
            except Exception: pass
        link_cols = list(dict.fromkeys(link_cols))

    rows = []
    for _, row in df.iterrows():
        nome = clean_text(row.get(name_col, ''))
        for lc in link_cols:
            url = clean_text(row.get(lc, ''))
            if url.startswith('http'):
                rows.append({'produto': nome, 'url': url, 'fonte_coluna': lc})
    return pd.DataFrame(rows)

def scrape_products(input_path: str, output_excel: str, headless: bool = True, shipping_cep: Optional[str] = None):
    """Percorre as URLs da planilha e devolve DataFrame de resultados + salva xlsx."""
    raw = pd.read_excel(input_path)
    items = normalize_input_dataframe(raw)
    if items.empty: raise RuntimeError('Nenhum link válido encontrado.')
    driver = None
    results = []
    try:
        driver = ensure_driver(driver, headless=headless)
        for _, r in items.iterrows():
            produto = r['produto']; url = r['url']; fonte = r['fonte_coluna']
            data = scrape_one(url, driver, cep=shipping_cep or CEP_DEFAULT)
            results.append({
                'produto': produto, 'url': url, 'fonte_coluna': fonte,
                **data
            })
            time.sleep(0.5 + random.random()*0.7)  # polidez
    finally:
        try:
            if driver is not None: driver.quit()
        except Exception: pass
    out = pd.DataFrame(results)
    out.to_excel(output_excel, index=False)
    return out, output_excel

# =========================
# CLI
# =========================
def parse_args():
    ap = argparse.ArgumentParser(description="Scraping + Ingest em MySQL (pipeline único).")
    ap.add_argument("--in", dest="in_path", default="produtos.xlsx", help="Planilha de entrada com nomes/URLs (xlsx).")
    ap.add_argument("--out-xlsx", dest="out_xlsx", default="produtos_scrape.xlsx", help="Arquivo Excel de saída.")
    ap.add_argument("--headless", dest="headless", default="1", help="1/0 para rodar sem interface (padrão 1).")
    ap.add_argument("--cep", dest="cep", default=CEP_DEFAULT, help="CEP para cálculo de frete (quando aplicável).")
    return ap.parse_args()

def main():
    args = parse_args()
    headless = str(args.headless).lower() in ("1","true","yes","y")

    print("[SCRAPE] Iniciando scraping...")
    df, xlsx = scrape_products(args.in_path, args.out_xlsx, headless=headless, shipping_cep=args.cep)
    print(f"[SCRAPE] Linhas coletadas: {len(df)} | Excel: {xlsx}")

    print("[INGEST] Normalizando e inserindo no MySQL...")
    ingest_dataframe(df)
    print("[OK] Pipeline finalizado.")

if __name__ == "__main__":
    main()

import os
import re
import glob
import math
import hashlib
import warnings
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# =========================
# CONFIGURE HERE
# =========================
MYSQL_USER="root"
MYSQL_PASS="admin"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_DB="ecommerce_scraping"
input_files="./db"
GLOBS = ["produtos_scrape_*.xlsx"]

# =========================
# UTILS
# =========================
def slugify(s: str, max_len: int = 100) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s).strip("-")
    return s[:max_len]

def parse_price(value) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    s = str(value).strip()
    s = s.replace("R$", "").replace(".", "").replace(" ", "")
    s = s.replace(",", ".")
    try:
        return round(float(s), 2)
    except:
        return None

def parse_avaliacao(value) -> Optional[float]:
    import re, math
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None

    s = str(value).strip().lower()

    # Se parecer contagem ("27 avaliações") e NÃO houver escala, trata como None
    if "avaliaç" in s and ("de 5" not in s and "/5" not in s):
        return None

    # Padrão explícito "x de 5" ou "x/5"
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:de|/)\s*5\b", s)
    if m:
        try:
            v = float(m.group(1).replace(",", "."))
            return round(v, 2) if 0 <= v <= 5 else None
        except:
            return None

    # Número solto (ex.: "4,3")
    m = re.search(r"\b(\d+(?:[.,]\d+)?)\b", s)
    if m:
        try:
            v = float(m.group(1).replace(",", "."))
            # Ratings devem estar entre 0 e 5; fora disso, considere inválido
            return round(v, 2) if 0 <= v <= 5 else None
        except:
            return None

    return None


BRANDS = [
    "Apple", "Samsung", "Motorola", "Xiaomi", "Nokia", "Asus", "Google", "Sony",
    "LG", "Realme", "OnePlus", "Huawei", "Infinix", "OPPO", "Vivo", "Lenovo"
]

def extract_product_fields(produto: str) -> Tuple[str, str, str]:
    """
    Very simple heuristics:
      - brand: first known brand substring found (case-insensitive)
      - model: first sequence after brand up to first " - | , (" or storage marker
      - variant: storage/color if present (e.g., 128GB / 256GB, color words)
    """
    if not produto:
        return ("Desconhecida", produto or "", "")
    brand = "Desconhecida"
    p = produto.strip()

    found = None
    for b in BRANDS:
        if re.search(rf"\b{re.escape(b)}\b", p, flags=re.IGNORECASE):
            found = b
            break
    if found:
        brand = found

    # Extract variant hints (storage or color words)
    variant_parts = []
    storage = re.findall(r"\b(\d{2,4}\s?GB)\b", p, flags=re.IGNORECASE)
    if storage:
        variant_parts.append(storage[0].upper())

    color = re.findall(r"\b(preto|black|azul|blue|verde|green|branco|white|cinza|gray|graphite|violet|violeta|pink|rosa)\b", p, flags=re.IGNORECASE)
    if color:
        variant_parts.append(color[0].capitalize())

    variant = " ".join(dict.fromkeys(variant_parts)) if variant_parts else ""

    # basic model extraction: remove brand and common filler tokens
    model = p
    if found:
        model = re.split(rf"\b{re.escape(found)}\b", p, flags=re.IGNORECASE, maxsplit=1)[-1].strip()
    model = re.split(r"[-|,(]", model)[0].strip()
    # compress redundant spaces
    model = re.sub(r"\s{2,}", " ", model)

    # If model is too short or equals to product, fallback
    if not model or len(model) < 3:
        model = p
    return (brand, model[:255], variant[:255])

@dataclass
class RowNormalized:
    url: Optional[str]
    fornecedor_name: Optional[str]
    seller_name: Optional[str]
    produto: Optional[str]
    price: Optional[float]
    avaliacao: Optional[float]
    frete_price: Optional[float]
    prazo_entrega: Optional[str]
    created_at: Optional[pd.Timestamp]

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "produto": "produto",
        "url": "url",
        "fornecedor": "fornecedor",
        "fonte_coluna": "fonte_coluna",
        "seller": "seller",
        "preco_num": "price_num",
        "preco": "preco_texto",
        "avaliacao": "avaliacao",
        "avaliacoes_qtd": "avaliacoes_qtd",
        "frete_valor": "frete_price",
        "frete_prazo": "prazo_entrega",
        "frete_metodo": "frete_metodo",
        "data_coleta": "created_at",
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    df = df.rename(columns=cols)

    # Compose price if numeric not provided
    if "price_num" in df.columns:
        df["price"] = df["price_num"].apply(parse_price)
    else:
        df["price"] = df.get("preco_texto", pd.Series([None]*len(df))).apply(parse_price)

    # Parse evaluation
    df["avaliacao_val"] = df.get("avaliacao", pd.Series([None]*len(df))).apply(parse_avaliacao)

    # Frete
    df["frete_price_val"] = df.get("frete_price", pd.Series([None]*len(df))).apply(parse_price)

    # Fornecedor and Seller
    df["fornecedor_name"] = df.get("fornecedor") if "fornecedor" in df.columns else df.get("fonte_coluna")
    df["seller_name"] = df.get("seller") if "seller" in df.columns else df["fornecedor_name"]

    # Created at
    if "created_at" in df.columns:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df["created_at_ts"] = pd.to_datetime(df["created_at"], errors="coerce")
    else:
        df["created_at_ts"] = pd.Timestamp.utcnow()

    keep = [
        "url", "fornecedor_name", "seller_name", "produto",
        "price", "avaliacao_val", "frete_price_val", "prazo_entrega", "created_at_ts"
    ]
    for k in keep:
        if k not in df.columns:
            df[k] = None

    out = df[keep].copy()
    out = out.dropna(subset=["produto", "fornecedor_name"], how="any")
    return out

# =========================
# DB HELPERS
# =========================
def get_engine() -> Engine:
    uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

def get_or_create_fornecedor(engine: Engine, name: str) -> int:
    code = slugify(name)
    with engine.begin() as conn:
        # try select by code
        row = conn.execute(text("SELECT id_fornecedor FROM Fornecedores WHERE code=:code"), {"code": code}).fetchone()
        if row:
            return row[0]
        conn.execute(
            text("INSERT INTO Fornecedores (name, code) VALUES (:name, :code)"),
            {"name": name.strip()[:255], "code": code[:100]}
        )
        row = conn.execute(text("SELECT id_fornecedor FROM Fornecedores WHERE code=:code"), {"code": code}).fetchone()
        return row[0]

def get_or_create_seller(engine: Engine, name: str, fk_fornecedor: int) -> int:
    # we don't have a uniqueness; approximate by pair (name, fk_fornecedor)
    with engine.begin() as conn:
        row = conn.execute(
            text("""SELECT id_seller FROM Seller 
                    WHERE name=:name AND fk_fornecedor=:fk"""),
            {"name": name.strip()[:255], "fk": fk_fornecedor}
        ).fetchone()
        if row:
            return row[0]
        conn.execute(
            text("""INSERT INTO Seller (name, fk_fornecedor) VALUES (:name, :fk)"""),
            {"name": name.strip()[:255], "fk": fk_fornecedor}
        )
        row = conn.execute(
            text("""SELECT id_seller FROM Seller 
                    WHERE name=:name AND fk_fornecedor=:fk"""),
            {"name": name.strip()[:255], "fk": fk_fornecedor}
        ).fetchone()
        return row[0]

def get_or_create_product(engine: Engine, produto: str) -> int:
    brand, model, variant = extract_product_fields(produto)
    # product code: stable hash of normalized product string
    norm = re.sub(r"\s+", " ", produto.strip().lower())
    code_raw = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:20]
    code = f"p_{code_raw}"
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id_product FROM Products WHERE code=:code"), {"code": code}).fetchone()
        if row:
            return row[0]
        conn.execute(
            text("""INSERT INTO Products (brand, code, model, variante) 
                    VALUES (:brand, :code, :model, :var)"""),
            {"brand": brand[:255], "code": code[:100], "model": model[:255], "var": variant[:255]}
        )
        row = conn.execute(text("SELECT id_product FROM Products WHERE code=:code"), {"code": code}).fetchone()
        return row[0]

def insert_list_row(engine: Engine, url: Optional[str], price: Optional[float], avaliacao: Optional[float],
                    frete_price: Optional[float], prazo_entrega: Optional[str], created_at, 
                    fk_product: int, fk_seller: int, fk_fornecedor: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""INSERT INTO List 
                (url, price, avaliacao, frete_price, prazo_entrega, created_at, fk_product, fk_seller, fk_fornecedor)
                VALUES (:url, :price, :avaliacao, :frete_price, :prazo, :created_at, :fk_product, :fk_seller, :fk_fornecedor)
            """),
            {
                "url": (url or "")[:500],
                "price": price if price is not None else None,
                "avaliacao": avaliacao if avaliacao is not None else None,
                "frete_price": frete_price if frete_price is not None else None,
                "prazo": (prazo_entrega or "")[:100] if prazo_entrega else None,
                "created_at": pd.to_datetime(created_at) if created_at is not None else None,
                "fk_product": fk_product,
                "fk_seller": fk_seller,
                "fk_fornecedor": fk_fornecedor,
            }
        )

def load_all_files() -> pd.DataFrame:
    paths = []
    for pattern in GLOBS:
        paths.extend(glob.glob(os.path.join(input_files, pattern)))
    if not paths:
        raise SystemExit(f"Nenhum arquivo encontrado em {input_files} com padrões: {GLOBS}")
    frames = []
    for p in sorted(paths):
        try:
            if p.lower().endswith(".xlsx"):
                df = pd.read_excel(p)
            else:
                df = pd.read_csv(p, sep=",", encoding="utf-8", on_bad_lines="skip")
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Falha ao ler {p}: {e}")
    if not frames:
        raise SystemExit("Nenhum DataFrame válido foi lido.")
    df_all = pd.concat(frames, ignore_index=True)
    return df_all

def main():
    print("[1/5] Lendo arquivos...")
    raw = load_all_files()
    print(f"   Linhas lidas: {len(raw)}")

    print("[2/5] Normalizando colunas...")
    norm = normalize_df(raw)
    print(f"   Linhas após normalização: {len(norm)}")

    engine = get_engine()
    print("[3/5] Conectado ao MySQL. Iniciando upserts...")

    ins_ok = 0
    errs = 0
    for idx, row in norm.iterrows():
        try:
            fornecedor_name = str(row["fornecedor_name"]).strip()
            seller_name = str(row["seller_name"]).strip() if row["seller_name"] else fornecedor_name
            produto = str(row["produto"]).strip()

            fk_fornecedor = get_or_create_fornecedor(engine, fornecedor_name)
            fk_seller = get_or_create_seller(engine, seller_name, fk_fornecedor)
            fk_product = get_or_create_product(engine, produto)

            insert_list_row(
                engine=engine,
                url=row["url"] if pd.notna(row["url"]) else None,
                price=row["price"] if pd.notna(row["price"]) else None,
                avaliacao=row["avaliacao_val"] if pd.notna(row["avaliacao_val"]) else None,
                frete_price=row["frete_price_val"] if pd.notna(row["frete_price_val"]) else None,
                prazo_entrega=row["prazo_entrega"] if pd.notna(row["prazo_entrega"]) else None,
                created_at=row["created_at_ts"] if pd.notna(row["created_at_ts"]) else None,
                fk_product=fk_product,
                fk_seller=fk_seller,
                fk_fornecedor=fk_fornecedor
            )
            ins_ok += 1
        except Exception as e:
            errs += 1
            # keep going
            print(f"[ERRO] Linha {idx}: {e}")

    print("[4/5] Concluído.")
    print(f"   Inserções em List: {ins_ok}")
    print(f"   Erros: {errs}")
    print("[5/5] FIM.")

if __name__ == "__main__":
    main()

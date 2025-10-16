# pip install playwright beautifulsoup4 lxml pandas
# python -m playwright install chromium

import re, json, time, unicodedata, html as ihtml
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Set
from urllib.parse import quote_plus

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ------------------------ Utils ------------------------

PRICE_RE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}")
RATING_VALUE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:de)?\s*5")
RATING_COUNT_RE = re.compile(r"(\d{1,3}(?:\.\d{3})+|\d+)")
TERMOS_EXCLUSAO = {
        "capa", "capinha", "película", "pelicula", "case", "acessórios", "protetor de tela"
    }

def clean_text(txt: str) -> str:
    return " ".join(ihtml.unescape(txt or "").split())

def extract_price(text: str) -> str | None:
    m = PRICE_RE.search(text or "")
    return clean_text(m.group(0)) if m else None

def sanitize_filename(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Za-z0-9\-_]+", "_", s.strip())
    return s.lower().strip("_")

def wait_any_selector(page, selectors: List[str], timeout_ms=60000) -> bool:
    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=timeout_ms)
            return True
        except PWTimeoutError:
            continue
    return False

def need_captcha_intervention(page) -> bool:
    title = (page.title() or "").lower()
    if any(x in title for x in ["captcha", "verify", "radware", "are you a robot", "verificação"]):
        return True
    if page.locator('iframe[src*="hcaptcha.com"], iframe[src*="recaptcha"]').count() > 0:
        return True
    if page.locator("text=/.*CAPTCHA.*/i").count() > 0:
        return True
    return False

def solve_if_captcha(page):
    if need_captcha_intervention(page):
        print("\n[ATENÇÃO] Verificação/CAPTCHA detectada.")
        print("→ Resolva manualmente na janela aberta. Quando a lista de produtos aparecer, pressione ENTER aqui no terminal.")
        try:
            input()
        except KeyboardInterrupt:
            raise SystemExit

def to_float_pt(v: str | None) -> str:
    if not v: return ""
    v = v.strip().replace(".", "").replace(",", ".")
    try:
        return f"{float(v):.2f}"
    except:
        return ""

def parse_rating_text(text: str) -> Tuple[str, str]:
    """Extrai (ratingValue, reviewCount) de blocos tipo '4,7 de 5 estrelas (1.234)'."""
    if not text: return ("","")
    val = ""
    cnt = ""
    m1 = RATING_VALUE_RE.search(text)
    if m1:
        val = to_float_pt(m1.group(1))
    m2 = RATING_COUNT_RE.search(text)
    if m2:
        raw = m2.group(1).replace(".", "")
        cnt = raw if raw.isdigit() else ""
    return (val, cnt)

def first_text(soup: BeautifulSoup, selectors: List[str]) -> str:
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(" ", strip=True)
            if t: return t
    return ""

def json_get(dct, *path, default=None):
    cur = dct
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

def normalize_money_like(val: str | None) -> str:
    if not val: return ""
    return clean_text(val)

# ---- NEW helpers (link absoluto, wait networkidle, auto-scroll, 'Vendido por') ----

def make_absolute(url: str, base: str) -> str:
    if not url: return ""
    if url.startswith("http"): return url
    if url.startswith("//"): return "https:" + url
    if url.startswith("/"):
        from urllib.parse import urlparse, urljoin
        pu = urlparse(base)
        return urljoin(f"{pu.scheme}://{pu.netloc}", url)
    return url

def smart_wait_networkidle(page, timeout_ms=120000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass  # alguns sites nunca entram em 'networkidle'

def auto_scroll(page, steps=8, delay=250):
    h_prev = 0
    for _ in range(steps):
        page.evaluate("window.scrollBy(0, document.body.scrollHeight/3)")
        time.sleep(delay/1000.0)
        h = page.evaluate("document.body.scrollHeight")
        if h == h_prev:
            break
        h_prev = h

def parse_vendido_por(text: str) -> str:
    """Extrai 'Vendido por X' / 'Sold by X' de blocos longos."""
    if not text: return ""
    t = clean_text(text)
    for pat in [r"Vendido por\s*[:\-]?\s*(.+?)(\s*\.|$)",
                r"Sold by\s*[:\-]?\s*(.+?)(\s*\.|$)"]:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            return clean_text(m.group(1))
    return ""

# ------------------------ JSON-LD extractor ------------------------

def extract_with_jsonld(soup: BeautifulSoup) -> List[Dict[str, str]]:
    results, seen = [], set()

    def add(item):
        name = item.get("nome")
        price = item.get("preco")
        link = item.get("link","")
        if not name or not price:
            return
        key = (name, price, link or "")
        if key in seen:
            return
        seen.add(key)
        results.append({
            "nome": clean_text(name),
            "preco": normalize_money_like(price),
            "link": link or "",
            "avaliacao": item.get("avaliacao",""),
            "avaliacoes": item.get("avaliacoes",""),
            "fornecedor": item.get("fornecedor",""),
        })

    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict): 
                continue

            def build_from_product(prod: Dict) -> Dict[str,str]:
                out = {}
                out["nome"] = prod.get("name")
                out["link"] = prod.get("url","")
                offers = prod.get("offers")
                price = None
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                elif isinstance(offers, list) and offers:
                    price = (offers[0] or {}).get("price")
                if price and re.match(r"^\d+(?:\.\d+)?$", str(price)):
                    price = f"R$ {str(price).replace('.', ',')}"
                out["preco"] = price or ""

                ar = prod.get("aggregateRating") or {}
                rating_val = json_get(ar, "ratingValue", default="")
                rating_cnt = json_get(ar, "reviewCount", default="") or json_get(ar, "ratingCount", default="")
                out["avaliacao"] = to_float_pt(str(rating_val)) if rating_val else ""
                out["avaliacoes"] = str(rating_cnt) if rating_cnt else ""

                brand = prod.get("brand")
                seller = prod.get("seller")
                seller_name = ""
                if isinstance(seller, dict):
                    seller_name = seller.get("name","")
                elif isinstance(seller, str):
                    seller_name = seller
                brand_name = ""
                if isinstance(brand, dict):
                    brand_name = brand.get("name","")
                elif isinstance(brand, str):
                    brand_name = brand
                out["fornecedor"] = seller_name or brand_name or ""
                return out

            if node.get("@type") == "ItemList" and isinstance(node.get("itemListElement"), list):
                for li in node["itemListElement"]:
                    prod = (li or {}).get("item") or {}
                    add(build_from_product(prod))
            elif node.get("@type") == "Product":
                add(build_from_product(node))
    return results

# ------------------------ Parsers por loja ------------------------

def parse_magalu(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out, seen = [], set()
    cards = []
    cards += soup.select('[data-testid*="product"], [data-test*="product"]')
    cards += [a.parent for a in soup.select('a[href*="/produto/"], a[href*="/p/"]') if a.parent]
    if not cards: cards = soup.select("li, div")
    for c in cards:
        name_el = c.select_one("h2, h3, [data-testid*='title'], .product-title, a[title], a[aria-label]")
        price_el = c.select_one("[data-testid*='price'], .price, [class*='preco'], [class*='price']")
        link_el  = c.select_one('a[href*="/produto/"], a[href*="/p/"]') or c.select_one("a[title], a[aria-label]")
        name = (name_el.get("title") or name_el.get("aria-label") or name_el.get_text(" ", strip=True)) if name_el else None
        link = link_el.get("href") if link_el and link_el.has_attr("href") else ""
        txt  = c.get_text(" ", strip=True)
        price = extract_price(price_el.get_text(" ", strip=True) if price_el else txt)
        if name and price:
            key = (name, price, link)
            if key not in seen:
                seen.add(key)
                out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link, "avaliacao":"", "avaliacoes":"", "fornecedor":""})
    if not out:
        out = extract_with_jsonld(soup)
    return out

def parse_kabum(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out, seen = [], set()
    cards = soup.select('[data-testid*="product-card"], .productCard, li.productGridItem, div.productCard')
    if not cards:
        cards = soup.select("li, div")
    for c in cards:
        name_el = c.select_one("h3, h2, .nameCard, a[title], .productCard__title")
        link_el = c.select_one("a[href]")
        price_el = c.select_one(".priceCard, [class*='price'] , .finalPrice, .productCard__price--final")
        txt = c.get_text(" ", strip=True)
        name = (name_el.get("title") or name_el.get_text(" ", strip=True)) if name_el else None
        link = link_el.get("href") if link_el and link_el.has_attr("href") else ""
        price = extract_price(price_el.get_text(" ", strip=True) if price_el else txt)
        if name and price:
            key = (name, price, link)
            if key not in seen:
                seen.add(key)
                out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link, "avaliacao":"", "avaliacoes":"", "fornecedor":""})
    if not out:
        out = extract_with_jsonld(soup)
    return out

def parse_amazon(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out, seen = [], set()
    for c in soup.select("div.s-result-item[data-asin]"):
        name_el = c.select_one("h2 a.a-link-normal.a-text-normal, h2 a.a-link-normal, h2 a")
        name = name_el.get_text(" ", strip=True) if name_el else None
        link = ""
        if name_el and name_el.has_attr("href"):
            href = name_el.get("href")
            link = ("https://www.amazon.com.br" + href) if href.startswith("/") else href
        price_block = c.select_one(".a-price")
        price_txt = price_block.get_text(" ", strip=True) if price_block else c.get_text(" ", strip=True)
        price = extract_price(price_txt)

        rating_txt = ""
        r_alt = c.select_one(".a-icon-alt")
        if r_alt:
            rating_txt = r_alt.get_text(" ", strip=True)
        count_txt = ""
        r_cnt = c.select_one(".s-link-style .s-underline-text, .a-size-base.s-underline-text")
        if r_cnt:
            count_txt = r_cnt.get_text(" ", strip=True)
        rating_val, rating_count = parse_rating_text(" ".join([rating_txt, count_txt]))

        if name and price:
            key = (name, price, link)
            if key not in seen:
                seen.add(key)
                out.append({
                    "nome": clean_text(name),
                    "preco": clean_text(price),
                    "link": link,
                    "avaliacao": rating_val,
                    "avaliacoes": rating_count,
                    "fornecedor": "",
                })
    if not out:
        out = extract_with_jsonld(soup)
    return out

def parse_mercado_livre(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out, seen = [], set()
    cards = soup.select("li.ui-search-layout__item, .ui-search-result__wrapper, .ui-search-result")
    if not cards:
        cards = soup.select("li, div")
    for c in cards:
        name_el = c.select_one("h2.ui-search-item__title, .ui-search-item__title, .poly-card__title")
        link_el = c.select_one("a.ui-search-link, a.poly-card__content")
        price_el = c.select_one("span.andes-money-amount, .ui-search-price__second-line, .andes-money-amount__fraction")
        txt = c.get_text(" ", strip=True)
        name = name_el.get_text(" ", strip=True) if name_el else None
        link = link_el.get("href") if link_el and link_el.has_attr("href") else ""
        price = extract_price(price_el.get_text(" ", strip=True) if price_el else txt)
        if name and price:
            key = (name, price, link)
            if key not in seen:
                seen.add(key)
                out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link, "avaliacao":"", "avaliacoes":"", "fornecedor":""})
    if not out:
        out = extract_with_jsonld(soup)
    return out

# ------------------------ Navegação por loja ------------------------

def magalu_url(term: str, page: int) -> str:
    base = f"https://www.magazineluiza.com.br/busca/{quote_plus(term)}/"
    return base if page == 1 else f"{base}?page={page}"

def kabum_url(term: str, page: int) -> str:
    return f"https://www.kabum.com.br/busca/{quote_plus(term)}?page_number={page}"

def amazon_url(term: str, page: int) -> str:
    return f"https://www.amazon.com.br/s?k={quote_plus(term)}&page={page}"

def mercado_livre_url(term: str, page: int) -> str:
    offset = 0 if page <= 1 else (page - 1) * 50 + 1
    base = f"https://lista.mercadolivre.com.br/{quote_plus(term)}"
    return base if page == 1 else f"{base}_Desde_{offset}"

SITES = {
    "magalu":        {"url": magalu_url,        "parse": parse_magalu,        "selectors": ['[data-testid*="product"]', 'a[href*="/produto/"]', '.product-title']},
    "kabum":         {"url": kabum_url,         "parse": parse_kabum,         "selectors": ['[data-testid*="product-card"]', '.productCard', 'a[href*="/produto/"]', 'a[href*="/produto"]']},
    "amazon":        {"url": amazon_url,        "parse": parse_amazon,        "selectors": ['div.s-result-item[data-asin]', '.a-price']},
    "mercado_livre": {"url": mercado_livre_url, "parse": parse_mercado_livre, "selectors": ['.ui-search-layout__item', '.ui-search-item__title']},
}

# ------------------------ Modo detalhes (ativado) ------------------------

FETCH_DETAILS = True  # ativado por padrão

DETAIL_SELECTORS = {
    "amazon": {
        "seller": [
            "#sellerProfileTriggerId",
            "#tabular-buybox .tabular-buybox-text a",
            "#merchant-info a",
            "#merchant-info"
        ],
        "rating": [
            "span[data-hook='rating-out-of-text']",
            "#averageCustomerReviews .a-icon-alt"
        ],
        "rating_count": [
            "#acrCustomerReviewText",
            "a[data-hook='see-all-reviews-link-foot']"
        ],
    },
    "magalu": {
        "seller": [
            "[data-testid='marketplace-seller-name']",
            "[data-testid='seller-name']",
            ".seller-name",
            "[data-testid*='marketplace']"
        ],
        "rating": [
            "meta[itemprop='ratingValue']",
            "[itemprop='ratingValue']",
            "[data-testid*='review-score']"
        ],
        "rating_count": [
            "meta[itemprop='reviewCount']",
            "[itemprop='reviewCount']",
            "[data-testid*='reviews-count']"
        ],
    },
    "kabum": {
        "seller": [
            ".sold-and-delivery__name",
            ".seller-name",
            "[data-testid*='seller']",
            "[class*='Seller']"
        ],
        "rating": [
            "meta[itemprop='ratingValue']",
            "[itemprop='ratingValue']",
            ".review-average"
        ],
        "rating_count": [
            "meta[itemprop='reviewCount']",
            "[itemprop='reviewCount']",
            ".review-count"
        ],
    },
    "mercado_livre": {
        "seller": [
            ".ui-pdp-seller__header__title",
            "a[data-testid='seller-link'] span",
            ".ui-vip-profile-info__seller-name",
            ".ui-pdp-seller__sales-description"
        ],
        "rating": [
            "meta[itemprop='ratingValue']",
            "span.ui-pdp-review__rating",
            ".ui-pdp-review__ratings .average"
        ],
        "rating_count": [
            "meta[itemprop='reviewCount']",
            "span.ui-pdp-review__amount",
            "a[href$='#reviews'] .ui-pdp-review__amount"
        ],
    },
}

def enrich_from_details(context, site: str, url: str) -> Dict[str,str]:
    if not url:
        return {}
    page_tmp = None
    try:
        page_tmp = context.new_page()

        bases = {
            "amazon": "https://www.amazon.com.br/",
            "magalu": "https://www.magazineluiza.com.br/",
            "kabum": "https://www.kabum.com.br/",
            "mercado_livre": "https://www.mercadolivre.com.br/",
        }
        base = bases.get(site, "https://www.google.com/")
        url_abs = make_absolute(url, base)

        page_tmp.goto(url_abs, wait_until="domcontentloaded", timeout=90000)
        smart_wait_networkidle(page_tmp, 45000)
        solve_if_captcha(page_tmp)

        auto_scroll(page_tmp, steps=10, delay=250)
        smart_wait_networkidle(page_tmp, 30000)

        # seguir canonical se houver
        try:
            canon = page_tmp.locator("link[rel='canonical']").first
            if canon.count() > 0:
                href = canon.get_attribute("href") or ""
                if href:
                    href = make_absolute(href, url_abs)
                    if href != url_abs:
                        page_tmp.goto(href, wait_until="domcontentloaded", timeout=90000)
                        smart_wait_networkidle(page_tmp, 45000)
                        auto_scroll(page_tmp, steps=8, delay=250)
        except Exception:
            pass

        html2 = page_tmp.content()
        soup2 = BeautifulSoup(html2, "lxml")

        # 1) JSON-LD primeiro
        jld = extract_with_jsonld(soup2)
        best = jld[0] if jld else {}
        seller = best.get("fornecedor","")
        rating = best.get("avaliacao","")
        rcount = best.get("avaliacoes","")

        # 2) Fallback por seletores
        sels = DETAIL_SELECTORS.get(site, {})

        if not seller:
            seller_txt = first_text(soup2, sels.get("seller", []))
            if site == "amazon" and not seller_txt:
                mi = soup2.select_one("#merchant-info")
                if mi:
                    seller_txt = parse_vendido_por(mi.get_text(" ", strip=True))
            seller = clean_text(seller_txt) if seller_txt else seller

        if not rating:
            r_el = None
            if sels.get("rating"):
                for sel in sels["rating"]:
                    r_el = soup2.select_one(sel)
                    if r_el:
                        break
            if r_el and r_el.has_attr("content"):
                rating = to_float_pt(r_el["content"])
            else:
                r_txt = first_text(soup2, sels.get("rating", []))
                rating, _ = parse_rating_text(r_txt)

        if not rcount:
            rc_el = None
            if sels.get("rating_count"):
                for sel in sels["rating_count"]:
                    rc_el = soup2.select_one(sel)
                    if rc_el:
                        break
            rc_txt = ""
            if rc_el:
                rc_txt = rc_el.get_text(" ", strip=True)
                if rc_el.has_attr("content"):
                    rc_txt = rc_el["content"]
            _, rcount = parse_rating_text(rc_txt)
            if not rcount:
                m = re.search(r"(\d{1,3}(?:\.\d{3})+|\d+)", clean_text(rc_txt or ""))
                if m:
                    rcount = m.group(1).replace(".", "")

        page_tmp.close()
        return {"fornecedor": seller or "", "avaliacao": rating or "", "avaliacoes": rcount or ""}
    except Exception:
        try:
            if page_tmp: page_tmp.close()
        except Exception:
            pass
        return {}

# ------------------------ Pipeline principal ------------------------

def scrape_term(term: str, first_page: int, last_page: int, max_results_per_term: int = 200) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=80)
        context = browser.new_context(
            locale="pt-BR",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
        )
        page = context.new_page()
        collected_count = 0 # contador de itens
        for site, spec in SITES.items():
            url_builder = spec["url"]
            parser      = spec["parse"]
            sel_list    = spec["selectors"]

            print(f"\n========== {site.upper()} :: {term} ==========")
            seen: Set[Tuple[str, str, str]] = set()

            for pg in range(first_page, last_page + 1):
                if collected_count >= max_results_per_term:
                    print(f"[LIMITE] Limite de {max_results_per_term} atingido para o termo '{term}'. Parando busca no site {site}.")
                    break
                
                url = url_builder(term, pg)
                print(f"[INFO] {site} -> página {pg}: {url}")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=120000)
                except Exception as e:
                    print(f"[ERRO] Navegando para {url}: {e}")
                    continue

                solve_if_captcha(page)

                found = wait_any_selector(page, sel_list, timeout_ms=90000)
                if not found:
                    print("[WARN] Nenhum seletor típico visível; tentando extrair mesmo assim.")

                time.sleep(1.0)

                html = page.content()
                dump_name = f"dump_{site}_{sanitize_filename(term)}_p{pg}.html"
                Path(dump_name).write_text(html, encoding="utf-8")

                soup = BeautifulSoup(html, "lxml")
                items = parser(soup)
                print(f"[INFO] {site} p{pg}: {len(items)} itens")

                for it in items:
                    nome_produto = it.get("nome", "").lower()
                    termo_buscado = term.lower()
                    
                    if any(termo in nome_produto for termo in TERMOS_EXCLUSAO):
                        continue # Pula 
                    
                    if termo_buscado not in nome_produto:
                        continue # Pula
                    
                    k = (it.get("nome",""), it.get("preco",""), it.get("link",""))
                    if k in seen:
                        continue
                    

                    # normaliza link relativo -> absoluto
                    if it.get("link"):
                        bases = {
                            "amazon": "https://www.amazon.com.br/",
                            "magalu": "https://www.magazineluiza.com.br/",
                            "kabum": "https://www.kabum.com.br/",
                            "mercado_livre": "https://www.mercadolivre.com.br/",
                        }
                        it["link"] = make_absolute(it["link"], bases.get(site, "https://www.google.com/"))

                    # Enriquecimento (fornecedor + rating) abrindo a página do produto
                    if FETCH_DETAILS and (not it.get("fornecedor") or not it.get("avaliacao")) and it.get("link"):
                        more = enrich_from_details(context, site, it.get("link"))
                        if more:
                            it.setdefault("fornecedor", more.get("fornecedor",""))
                            it.setdefault("avaliacao", more.get("avaliacao",""))
                            it.setdefault("avaliacoes", more.get("avaliacoes",""))

                    seen.add(k)
                    rows.append({
                        "site": site,
                        "termo": term,
                        "nome": it.get("nome",""),
                        "preco": it.get("preco",""),
                        "link": it.get("link",""),
                        "avaliacao": it.get("avaliacao",""),
                        "avaliacoes": it.get("avaliacoes",""),
                        "fornecedor": it.get("fornecedor",""),
                    })

                time.sleep(1.0)

        browser.close()
    return rows

if __name__ == "__main__":
    termos = ["iphone 17"]   # <-- adicione mais termos se quiser
    first_page, last_page = 1, 1 # <-- ajuste o range de páginas
    MAX_PRODUTOS = 10 # <-- Define o limite máximo de produtos no resultado final
    all_rows: List[Dict[str,str]] = []

    for termo in termos:
        rows = scrape_term(termo, first_page, last_page)
        all_rows.extend(rows)
        print(f"[OK] {termo}: coletados {len(rows)} registros")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"precos_{sanitize_filename('_'.join(termos))}_{ts}.csv"
    df = pd.DataFrame(
        all_rows,
        columns=["site","termo","nome","preco","link","avaliacao","avaliacoes","fornecedor"]
    )
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n[FINALIZADO] CSV salvo: {out_csv}  (linhas: {len(df)})")

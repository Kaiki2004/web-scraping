# pip install playwright beautifulsoup4 lxml pandas
# python -m playwright install chromium

import re, json, time, csv, os, unicodedata, html as ihtml
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Set
from urllib.parse import quote_plus

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ------------------------ Utils ------------------------

PRICE_RE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}")

def clean_text(txt: str) -> str:
    return " ".join(ihtml.unescape(txt).split())

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
    # iframes/elementos comuns
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

def extract_with_jsonld(soup: BeautifulSoup) -> List[Dict[str, str]]:
    results, seen = [], set()
    def add(name, price, link=None):
        if not name or not price: return
        key = (name, price, link or "")
        if key in seen: return
        seen.add(key)
        results.append({"nome": clean_text(name), "preco": clean_text(price), "link": link or ""})

    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict): 
                continue
            if node.get("@type") == "ItemList" and isinstance(node.get("itemListElement"), list):
                for li in node["itemListElement"]:
                    item = (li or {}).get("item") or {}
                    name = item.get("name")
                    url  = item.get("url")
                    offers = item.get("offers")
                    price = None
                    if isinstance(offers, dict):
                        price = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                    elif isinstance(offers, list) and offers:
                        price = (offers[0] or {}).get("price")
                    if price and re.match(r"^\d+(?:\.\d+)?$", str(price)):
                        price = f"R$ {str(price).replace('.', ',')}"
                    add(name, price, url)
            if node.get("@type") == "Product":
                name = node.get("name")
                url  = node.get("url")
                offers = node.get("offers")
                price = None
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                elif isinstance(offers, list) and offers:
                    price = (offers[0] or {}).get("price")
                if price and re.match(r"^\d+(?:\.\d+)?$", str(price)):
                    price = f"R$ {str(price).replace('.', ',')}"
                add(name, price, url)
    return results

# ------------------------ Parsers por loja ------------------------
# Cada parser devolve lista de dicts: {"nome","preco","link"}

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
                out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link})
    if not out:
        out = extract_with_jsonld(soup)
    return out

def parse_kabum(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out, seen = [], set()
    # Cards comuns no Kabum
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
                seen.add(key); out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link})
    if not out:
        out = extract_with_jsonld(soup)
    return out

def parse_amazon(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out, seen = [], set()
    # Itens de busca: .s-result-item (com data-asin)
    for c in soup.select("div.s-result-item[data-asin]"):
        # Título
        name_el = c.select_one("h2 a.a-link-normal.a-text-normal, h2 a.a-link-normal, h2 a")
        name = name_el.get_text(" ", strip=True) if name_el else None
        # Link
        link = ""
        if name_el and name_el.has_attr("href"):
            href = name_el.get("href")
            link = ("https://www.amazon.com.br" + href) if href.startswith("/") else href
        # Preço (inteiro + fracionário)
        price_block = c.select_one(".a-price")
        price_txt = ""
        if price_block:
            price_txt = price_block.get_text(" ", strip=True)
        else:
            price_txt = c.get_text(" ", strip=True)
        price = extract_price(price_txt)
        if name and price:
            key = (name, price, link)
            if key not in seen:
                seen.add(key); out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link})
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
                seen.add(key); out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link})
    if not out:
        out = extract_with_jsonld(soup)
    return out

def parse_shopee(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out, seen = [], set()
    # Shopee costuma renderizar via JS. Quando renderiza server-side, seletores:
    cards = soup.select('a[href*="/product/"], a[href*="/search/"]')
    for c in cards:
        name = c.get("title") or c.get_text(" ", strip=True)
        link = c.get("href") or ""
        price = extract_price(c.get_text(" ", strip=True))
        if name and price:
            key = (name, price, link)
            if key not in seen:
                seen.add(key); out.append({"nome": clean_text(name), "preco": clean_text(price), "link": link})
    if not out:
        out = extract_with_jsonld(soup)
    return out

# ------------------------ Navegação por loja ------------------------

def magalu_url(term: str, page: int) -> str:
    base = f"https://www.magazineluiza.com.br/busca/{quote_plus(term)}/"
    return base if page == 1 else f"{base}?page={page}"

def kabum_url(term: str, page: int) -> str:
    # Estrutura comum: /busca/{termo}/?page_number=n  (varia; este costuma funcionar)
    return f"https://www.kabum.com.br/busca/{quote_plus(term)}?page_number={page}"

def amazon_url(term: str, page: int) -> str:
    return f"https://www.amazon.com.br/s?k={quote_plus(term)}&page={page}"

def mercado_livre_url(term: str, page: int) -> str:
    # paginação com _Desde_ (offset). 1->0, 2->51...
    offset = 0 if page <= 1 else (page - 1) * 50 + 1
    base = f"https://lista.mercadolivre.com.br/{quote_plus(term)}"
    return base if page == 1 else f"{base}_Desde_{offset}"

def shopee_url(term: str, page: int) -> str:
    # Shopee pagina zero-based (?page=0,1,2...):
    return f"https://shopee.com.br/search?keyword={quote_plus(term)}&page={max(0, page-1)}"

SITES = {
    "magalu":        {"url": magalu_url,        "parse": parse_magalu,        "selectors": ['[data-testid*="product"]', 'a[href*="/produto/"]', '.product-title']},
    "kabum":         {"url": kabum_url,         "parse": parse_kabum,         "selectors": ['[data-testid*="product-card"]', '.productCard', 'a[href*="/produto/"]', 'a[href*="/produto"]']},
    "amazon":        {"url": amazon_url,        "parse": parse_amazon,        "selectors": ['div.s-result-item[data-asin]', '.a-price']},
    "mercado_livre": {"url": mercado_livre_url, "parse": parse_mercado_livre, "selectors": ['.ui-search-layout__item', '.ui-search-item__title']},
    "shopee":        {"url": shopee_url,        "parse": parse_shopee,        "selectors": ['a[href*="/product/"]', 'a[title]']},
}

# ------------------------ Pipeline principal ------------------------

def scrape_term(term: str, first_page: int, last_page: int) -> List[Dict[str, str]]:
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

        for site, spec in SITES.items():
            url_builder = spec["url"]
            parser      = spec["parse"]
            sel_list    = spec["selectors"]

            print(f"\n========== {site.upper()} :: {term} ==========")
            seen: Set[Tuple[str, str, str]] = set()

            for pg in range(first_page, last_page + 1):
                url = url_builder(term, pg)
                print(f"[INFO] {site} -> página {pg}: {url}")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=120000)
                except Exception as e:
                    print(f"[ERRO] Navegando para {url}: {e}")
                    continue

                # Captcha?
                solve_if_captcha(page)

                # Espera algum seletor de produto (se nada, tenta mesmo assim).
                found = wait_any_selector(page, sel_list, timeout_ms=90000)
                if not found:
                    print("[WARN] Nenhum seletor típico visível; tentando extrair mesmo assim.")

                time.sleep(1.0)  # folga pro DOM estabilizar

                html = page.content()
                # dump por página para depuração opcional:
                dump_name = f"dump_{site}_{sanitize_filename(term)}_p{pg}.html"
                Path(dump_name).write_text(html, encoding="utf-8")

                soup = BeautifulSoup(html, "lxml")
                items = parser(soup)
                print(f"[INFO] {site} p{pg}: {len(items)} itens")

                for it in items:
                    k = (it.get("nome",""), it.get("preco",""), it.get("link",""))
                    if k in seen: 
                        continue
                    seen.add(k)
                    rows.append({
                        "site": site,
                        "termo": term,
                        "nome": it.get("nome",""),
                        "preco": it.get("preco",""),
                        "link": it.get("link",""),
                    })

                # Pause educada entre páginas
                time.sleep(1.0)

        browser.close()
    return rows

if __name__ == "__main__":
    termos = ["playstation 5"]             # <-- adicione mais termos se quiser
    first_page, last_page = 1, 3           # <-- ajuste o range de páginas
    all_rows: List[Dict[str,str]] = []

    for termo in termos:
        rows = scrape_term(termo, first_page, last_page)
        all_rows.extend(rows)
        print(f"[OK] {termo}: coletados {len(rows)} registros")

    # Salva CSV único com timestamp
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"precos_{sanitize_filename('_'.join(termos))}_{ts}.csv"
    df = pd.DataFrame(all_rows, columns=["site","termo","nome","preco","link"])
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n[FINALIZADO] CSV salvo: {out_csv}  (linhas: {len(df)})")

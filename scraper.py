

import os, re, time, random, sys, json
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

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

# ----------------------------- Constantes -----------------------------
CEP_DEFAULT = "14401-426"

CURRENCY_RE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}")
NUMBER_RE = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}")
RATING_RE = re.compile(r"(\d+[.,]\d+)\s*(?:/|de)?\s*5")
COUNT_RE = re.compile(r"(\d{1,3}(?:\.\d{3})*)")

BAD_PRICE_HINTS = (
    'de ', 'de:', 'antes', 'was', 'de r$', 'sem juros', 'juros', 'parcela', 'parcelas', 'em até', 'cartão', 'no cartão',
    'assinatura', 'subscribe', 'club', 'prime'
)
OLD_PRICE_CLASSES = ('old', 'from', 'risk', 'strike', 'line-through', 'cross', 'de-preco', 'preco-de', 'price-from')
GOOD_PRICE_HINTS = ('por ', 'à vista', 'a vista', 'pix', 'boleto', 'preço', 'price', 'valor', 'final', 'agora')

SELECTORS_PRICE_COMMON = [
    '[itemprop="price"]', '[data-testid*="price"]', '[class*="price"]', '[aria-label*="preço"]', '[aria-label*="preco"]'
]
SELECTORS_PRICE_MAGALU = [
    '[data-testid="price-value"]', '[data-testid="price-big"]', '[data-testid="price-amount"]', '[class*="Price"]'
]
SELECTORS_PRICE_KABUM = [
    '[data-testid="product-price"]', 'span.finalPrice', 'h4.finalPrice', '.priceCard strong'
]

SELECTORS_SHIPPING_LINES = [
    '[data-testid*="shipping"], .shipping-option, .frete-opcao, li:has([class*="frete"])',
    '.modal [class*="frete"] li', '.delivery-options li', '.freight-options li', '.frete li'
]
SELECTORS_CEP_INPUT = [
    'input[name*="cep"]', 'input[id*="cep"]', 'input[placeholder*="CEP"]', 'input[aria-label*="CEP"]',
    'input[name*="zip"]', 'input[id*="zip"]', 'input[placeholder*="Código postal"]', 'input[aria-label*="zip"]'
]

# ----------------------------- Utils -----------------------------
def clean_text(s):
    if not s: return ''
    return re.sub(r"\s+", " ", str(s)).strip()

def br_to_float(txt):
    if not txt: return None
    m = NUMBER_RE.search(txt)
    if not m: return None
    num = m.group(0).replace('.', '').replace(',', '.')
    try: return float(num)
    except Exception: return None

def norm_price_str(txt):
    m = NUMBER_RE.search(txt)
    if not m: return ''
    return f"R$ {m.group(0)}"

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
    out = []
    for obj in extract_jsonld_all(soup):
        try:
            offers = obj.get('offers') if isinstance(obj, dict) else None
            if offers:
                if isinstance(offers, list):
                    for o in offers:
                        for key in ('price','lowPrice','highPrice'):
                            v = o.get(key) if isinstance(o, dict) else None
                            if isinstance(v,(int,float,str)): out.append(str(v))
                elif isinstance(offers, dict):
                    for key in ('price','lowPrice','highPrice'):
                        v = offers.get(key)
                        if isinstance(v,(int,float,str)): out.append(str(v))
        except Exception:
            pass
    normed = []
    for v in out:
        v = str(v)
        if v and ',' in v and NUMBER_RE.search(v):
            normed.append(norm_price_str(v))
        else:
            try:
                f = float(v)
                br = f"R$ {f:,.2f}".replace(',', 'X').replace('.', ',').replace('X','.')
                normed.append(br)
            except Exception:
                pass
    return normed

def collect_dom_prices(soup, domain_hint=''):
    cands = []
    sel_list = []
    if 'magalu' in domain_hint or 'magazineluiza' in domain_hint: sel_list.extend(SELECTORS_PRICE_MAGALU)
    if 'kabum' in domain_hint: sel_list.extend(SELECTORS_PRICE_KABUM)
    sel_list.extend(SELECTORS_PRICE_COMMON)
    for sel in sel_list:
        for el in soup.select(sel):
            txt = clean_text(el.get_text(' '))
            if not txt: continue
            cls = ' '.join(el.get('class', [])).lower()
            if any(k in cls for k in OLD_PRICE_CLASSES): continue
            if not CURRENCY_RE.search(txt) and not NUMBER_RE.search(txt): continue
            score = 0; low = txt.lower()
            if any(h in low for h in GOOD_PRICE_HINTS): score += 2
            if any(h in low for h in BAD_PRICE_HINTS): score -= 2
            if sel in SELECTORS_PRICE_MAGALU or sel in SELECTORS_PRICE_KABUM: score += 2
            cands.append((txt, score, f'sel:{sel}'))
    for el in soup.find_all(string=re.compile(r"(por|à vista|a vista|pix)", re.I)):
        txt_block = clean_text(el.parent.get_text(' ')) if el and hasattr(el,'parent') else ''
        if txt_block and (CURRENCY_RE.search(txt_block) or NUMBER_RE.search(txt_block)):
            cands.append((txt_block, 3, 'context:por/avista'))
    return cands

def pick_best_price(cands):
    normed = []
    for txt, score, reason in cands:
        low = txt.lower()
        if any(k in low for k in ('x de r$', 'x de ', 'em até', 'parcela', 'parcelas')): continue
        if (' de ' in low or ' de r$' in low) and ' por ' not in low: score -= 2
        price_str = norm_price_str(txt); price_num = br_to_float(price_str)
        if not price_str or price_num is None: continue
        normed.append((price_str, price_num, score, reason))
    if not normed: return '', None, []
    normed.sort(key=lambda x: (-x[2], x[1]))
    best = normed[0]
    debug = [f"{p} | {v} | score={s} | {r}" for (p, v, s, r) in normed[:6]]
    return best[0], best[1], debug

def clean_seller(s):
    s = clean_text(s)
    if not s: return ''
    low = s.lower()
    lixo = ['cadastre', 'login', 'entrar', 'crie sua conta', 'assine', 'newsletter', 'oferta exclusiva', 'receba ofertas']
    if any(w in low for w in lixo): return ''
    for prefix in ['Vendido e entregue por', 'Vendido por', 'Entregue por', 'loja:']:
        if low.startswith(prefix.lower()):
            s = s[len(prefix):].strip(' :.-')
    return s

def extract_jsonld_rating_and_count(soup):
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
                        cnt_s = str(cnt)
                        m = COUNT_RE.search(cnt_s)
                        if m: count_val = int(m.group(1).replace('.', ''))
        except Exception:
            pass
    return rating_val, count_val

def normalize_rating(r):
    r = clean_text(r)
    m = re.search(r"^\d+[.,]\d+$", r or '')
    if m:
        val = m.group(0).replace('.', ',')
        return f"{val} de 5"
    m2 = RATING_RE.search(r or '')
    if m2:
        val = m2.group(1).replace('.', ',')
        return f"{val} de 5"
    return r or ''

# ----------------------------- Selenium -----------------------------
def _build_options(headless=True):
    opts = Options()
    if headless: opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox'); opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu'); opts.add_argument('--window-size=1366,900')
    opts.add_argument('--lang=pt-BR')
    opts.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
    prefs = {"intl.accept_languages":"pt-BR,pt", "profile.default_content_setting_values.images":2}
    opts.add_experimental_option("prefs", prefs)
    return opts

def new_driver(headless=True):
    opts = _build_options(headless=headless)
    if USE_WDM:
        try:
            return webdriver.Chrome(options=opts)
        except Exception:
            return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    else:
        return webdriver.Chrome(options=opts)

def ensure_driver(driver, headless=True):
    try:
        if driver is None: return new_driver(headless=headless)
        _ = driver.current_url
        return driver
    except InvalidSessionIdException:
        try: driver.quit()
        except Exception: pass
        return new_driver(headless=headless)
    except WebDriverException:
        try: driver.quit()
        except Exception: pass
        return new_driver(headless=headless)

def click_popups(driver):
    try:
        labels = ['Aceitar', 'Aceitar todos', 'Concordo', 'Continuar', 'Fechar', 'OK', 'Entendi']
        for label in labels:
            for xp in [
                f'//button[normalize-space()="{label}"]',
                f'//button[contains(translate(., "ACEITARCONCORDOFECHAROKENTENDI", "aceitarconcordofecharokentendi"), "{label.lower()}")]',
            ]:
                for el in driver.find_elements(By.XPATH, xp):
                    try: el.click(); time.sleep(0.2)
                    except: pass
    except Exception:
        pass

def warmup(driver):
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.25);"); time.sleep(0.3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.6);"); time.sleep(0.3)
        driver.execute_script("window.scrollTo(0, 0);"); time.sleep(0.2)
    except JavascriptException:
        pass

def try_fill_cep_and_calculate(driver, cep, timeout=12):
    if not cep: 
        return False
    ok = False
    for xp in [
        '//button[contains(translate(.,"FRETEENTREGA","freteentrega"),"frete")]',
        '//button[contains(translate(.,"FRETEENTREGA","freteentrega"),"entrega")]',
        '//a[contains(translate(.,"FRETEENTREGA","freteentrega"),"frete")]'
    ]:
        try:
            for b in driver.find_elements(By.XPATH, xp)[:2]:
                try: b.click(); time.sleep(0.3)
                except: pass
        except Exception:
            pass

    inputs = []
    for css in SELECTORS_CEP_INPUT:
        try:
            inputs.extend(driver.find_elements(By.CSS_SELECTOR, css))
        except Exception:
            pass
    if not inputs:
        for xp in ['//input[contains(translate(@name,"CEPZIP","cepzip"),"cep") or contains(translate(@id,"CEPZIP","cepzip"),"cep") or contains(translate(@name,"CEPZIP","cepzip"),"zip") or contains(translate(@id,"CEPZIP","cepzip"),"zip")]']:
            try:
                inputs = driver.find_elements(By.XPATH, xp)
                if inputs: break
            except Exception:
                pass

    for inp in inputs[:3]:
        try:
            inp.clear(); time.sleep(0.2)
            inp.send_keys(cep); time.sleep(0.2)
            inp.send_keys(Keys.ENTER); time.sleep(0.5)
            ok = True
            break
        except Exception:
            continue

    for xp in [
        '//button[contains(translate(.,"CALCULAROKAPLICARCONFIRMAR","calcularokaplicarconfirmar"),"calcular")]',
        '//button[contains(translate(.,"CALCULAROKAPLICARCONFIRMAR","calcularokaplicarconfirmar"),"ok")]',
        '//button[contains(translate(.,"CALCULAROKAPLICARCONFIRMAR","calcularokaplicarconfirmar"),"aplicar")]',
        '//button[contains(translate(.,"CALCULAROKAPLICARCONFIRMAR","calcularokaplicarconfirmar"),"confirmar")]',
    ]:
        try:
            for b in driver.find_elements(By.XPATH, xp)[:2]:
                try: b.click(); time.sleep(0.4); ok = True
                except: pass
        except Exception:
            pass

    if ok:
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.XPATH, '//*[contains(translate(text(),"FRETEENTREGA","freteentrega"),"frete") or contains(translate(text(),"FRETEENTREGA","freteentrega"),"entrega")]'))
            )
        except TimeoutException:
            pass
    return ok

def parse_shipping_options(soup, html_text):
    lines = []
    for sel in SELECTORS_SHIPPING_LINES:
        lines.extend(soup.select(sel))
    if not lines:
        for tag in soup.find_all(string=re.compile(r"(frete|entrega|correios|transportadora)", re.I)):
            try:
                block = tag.find_parent(['li','div','tr'])
                if block and block not in lines:
                    lines.append(block)
            except Exception:
                pass

    def extract_price(txt):
        m = CURRENCY_RE.search(txt)
        if m: return m.group(0), br_to_float(m.group(0))
        if re.search(r'grátis|gratis', txt, flags=re.I): return 'R$ 0,00', 0.0
        return '', None

    cands = []
    for el in lines[:20]:
        txt = clean_text(el.get_text(" "))
        if not txt: continue
        price_str, price_num = extract_price(txt)
        if price_str or price_num == 0.0:
            prazo = ''
            metodo = ''
            m_prazo = re.search(r'(em até\s+\d+\s+dias? úteis?|chega\s+\w+|(\d+)\s+dias? úteis?)', txt, flags=re.I)
            if m_prazo: prazo = clean_text(m_prazo.group(0))
            m_met = re.search(r'(express[oa]|econômic[ao]|econômico|correios|sedex|pac|retirada|loja|transportadora)', txt, flags=re.I)
            if m_met: metodo = clean_text(m_met.group(0))
            cands.append((price_str or 'R$ 0,00', price_num if price_num is not None else 999999.99, metodo, prazo))

    if not cands:
        all_txt = soup.get_text(" ")
        if re.search(r'(frete|entrega)', all_txt, flags=re.I):
            price_strs = CURRENCY_RE.findall(all_txt)
            price_nums = [(ps, br_to_float(ps)) for ps in price_strs]
            price_nums = [(ps, pn) for ps, pn in price_nums if pn is not None]
            if price_nums:
                ps, pn = sorted(price_nums, key=lambda x: x[1])[0]
                cands.append((ps, pn, '', ''))

    if not cands:
        return '', '', ''

    cands.sort(key=lambda x: (x[1], x[2], x[3]))
    best = cands[0]
    return best[0], best[3], best[2]

# ----------------------------- Parsers por domínio -----------------------------
def parse_magalu(soup, html, driver=None, cep=None):
    prices = jsonld_prices(soup)
    cands = [(p, 5, 'jsonld') for p in prices] + collect_dom_prices(soup, 'magalu')
    price_str, price_num, debug = pick_best_price(cands)

    seller = ''
    for obj in extract_jsonld_all(soup):
        try:
            offers = obj.get('offers') if isinstance(obj, dict) else None
            if isinstance(offers, dict) and isinstance(offers.get('seller'), dict):
                seller = offers['seller'].get('name') or seller
        except Exception: pass
    if not seller:
        seller = clean_text(next((e.get_text() for e in soup.select('[data-testid="marketplace-seller-name"], [data-testid="store-link"]') if clean_text(e.get_text())), '')) or ('Magalu' if 'magalu' in html.lower() else '')
    seller = clean_seller(seller)

    # rating e count
    r_json, c_json = extract_jsonld_rating_and_count(soup)
    rating = normalize_rating(r_json) if r_json else ''
    count = c_json
    if not rating:
        rating = normalize_rating(next((e.get_text() for e in soup.select('[data-testid="review-summary-overall-rating"], [data-testid="rating-value"], [itemprop="ratingValue"]') if clean_text(e.get_text())), ''))
    if count is None:
        txts = [e.get_text() for e in soup.select('[itemprop="reviewCount"], [itemprop="ratingCount"], [data-testid*="review-count"], .review-count, .reviews-count')]
        for txt in txts:
            m = COUNT_RE.search(txt)
            if m: count = int(m.group(1).replace('.', '')); break
    if count is None:
        m = re.search(r'(\d{1,3}(?:\.\d{3})*)\s+aval(?:iaç|iac)ões', soup.get_text(' '), flags=re.I)
        if m: count = int(m.group(1).replace('.', ''))

    # frete
    frete_valor = frete_prazo = frete_metodo = ''
    if driver and cep and try_fill_cep_and_calculate(driver, cep):
        time.sleep(1.2)
        soup2 = BeautifulSoup(driver.page_source, 'html.parser')
        frete_valor, frete_prazo, frete_metodo = parse_shipping_options(soup2, driver.page_source)

    return price_str, seller, rating, count, frete_valor, frete_prazo, frete_metodo, debug

def parse_kabum(soup, html, driver=None, cep=None):
    prices = jsonld_prices(soup)
    cands = [(p, 5, 'jsonld') for p in prices] + collect_dom_prices(soup, 'kabum')
    price_str, price_num, debug = pick_best_price(cands)

    seller = ''
    for obj in extract_jsonld_all(soup):
        try:
            offers = obj.get('offers') if isinstance(obj, dict) else None
            if isinstance(offers, dict) and isinstance(offers.get('seller'), dict):
                seller = offers['seller'].get('name') or seller
        except Exception: pass
    if not seller:
        seller = clean_text(next((e.get_text() for e in soup.select('[data-testid="marketplace-seller-name"], .seller-name, a[href*="seller"]') if clean_text(e.get_text())), '')) or ('KaBuM!' if 'kabum' in html.lower() else '')
    seller = clean_seller(seller)

    r_json, c_json = extract_jsonld_rating_and_count(soup)
    rating = normalize_rating(r_json) if r_json else ''
    count = c_json
    if not rating:
        rating = normalize_rating(next((e.get_text() for e in soup.select('[data-testid="rating-value"], [itemprop="ratingValue"], .rating__value') if clean_text(e.get_text())), ''))
    if count is None:
        txts = [e.get_text() for e in soup.select('[itemprop="reviewCount"], [itemprop="ratingCount"], [data-testid*="review-count"], .review-count, .reviews-count')]
        for txt in txts:
            m = COUNT_RE.search(txt)
            if m: count = int(m.group(1).replace('.', '')); break
    if count is None:
        m = re.search(r'(\d{1,3}(?:\.\d{3})*)\s+aval(?:iaç|iac)ões', soup.get_text(' '), flags=re.I)
        if m: count = int(m.group(1).replace('.', ''))

    frete_valor = frete_prazo = frete_metodo = ''
    if driver and cep and try_fill_cep_and_calculate(driver, cep):
        time.sleep(1.2)
        soup2 = BeautifulSoup(driver.page_source, 'html.parser')
        frete_valor, frete_prazo, frete_metodo = parse_shipping_options(soup2, driver.page_source)

    return price_str, seller, rating, count, frete_valor, frete_prazo, frete_metodo, debug

def parse_generic(soup, html, driver=None, cep=None):
    prices = jsonld_prices(soup)
    cands = [(p, 5, 'jsonld') for p in prices] + collect_dom_prices(soup, '')
    price_str, price_num, debug = pick_best_price(cands)

    seller = ''
    for obj in extract_jsonld_all(soup):
        try:
            offers = obj.get('offers') if isinstance(obj, dict) else None
            if isinstance(offers, dict) and isinstance(offers.get('seller'), dict):
                seller = offers['seller'].get('name') or seller
        except Exception: pass
    if not seller:
        m = re.search(r'Vendido(?: e entregue)? por[: ]+([A-Za-z0-9\-\._\s]+)', html, flags=re.I)
        if m: seller = clean_seller(m.group(1))
    seller = clean_seller(seller)

    r_json, c_json = extract_jsonld_rating_and_count(soup)
    rating = normalize_rating(r_json) if r_json else ''
    count = c_json
    if not rating:
        rating = normalize_rating(next((e.get_text() for e in soup.select('[itemprop="ratingValue"], [class*="rating"], [aria-label*="avalia"]') if clean_text(e.get_text())), ''))
    if count is None:
        txts = [e.get_text() for e in soup.select('[itemprop="reviewCount"], [itemprop="ratingCount"], [data-testid*="review-count"], .review-count, .reviews-count')]
        for txt in txts:
            m = COUNT_RE.search(txt)
            if m: count = int(m.group(1).replace('.', '')); break
    if count is None:
        m = re.search(r'(\d{1,3}(?:\.\d{3})*)\s+aval(?:iaç|iac)ões', soup.get_text(' '), flags=re.I)
        if m: count = int(m.group(1).replace('.', ''))

    frete_valor = frete_prazo = frete_metodo = ''
    if driver and cep and try_fill_cep_and_calculate(driver, cep):
        time.sleep(1.2)
        soup2 = BeautifulSoup(driver.page_source, 'html.parser')
        frete_valor, frete_prazo, frete_metodo = parse_shipping_options(soup2, driver.page_source)

    return price_str, seller, rating, count, frete_valor, frete_prazo, frete_metodo, debug

DOMAIN_PARSERS = {
    'magazineluiza.com.br': parse_magalu,
    'magalu.com': parse_magalu,
    'kabum.com.br': parse_kabum,
}

def pick_parser(url):
    try:
        net = urlparse(url).netloc.lower()
    except Exception:
        return parse_generic
    if net.startswith('www.'): net = net[4:]
    for k, fn in DOMAIN_PARSERS.items():
        if k in net: return fn
    return parse_generic

def normalize_input_dataframe(df):
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

def ensure_driver_and_get(driver, url, headless=True, retries=2):
    for attempt in range(retries+1):
        driver = ensure_driver(driver, headless=headless)
        try:
            driver.get(url)
            click_popups(driver)
            warmup(driver)
            return driver, driver.page_source, None
        except InvalidSessionIdException as e:
            try: driver.quit()
            except Exception: pass
            driver = None
            time.sleep(0.5 + attempt*0.5)
            last_err = e
        except WebDriverException as e:
            last_err = e
            time.sleep(0.5 + attempt*0.5)
        except Exception as e:
            last_err = e
            time.sleep(0.5 + attempt*0.5)
    return driver, None, last_err

def scrape_products(input_path='produtos.xlsx', output_excel='produtos_scrape.xlsx', output_csv='produtos_scrape.csv', headless=True, shipping_cep=None):
    raw = pd.read_excel(input_path)
    items = normalize_input_dataframe(raw)
    if items.empty: raise RuntimeError('Nenhum link válido encontrado.')
    driver = None
    results = []
    try:
        for _, r in items.iterrows():
            produto = r['produto']; url = r['url']; fonte = r['fonte_coluna']
            started = datetime.now()
            status = 'ok'; erro = ''
            price = seller = rating = ''
            price_num = None; price_debug_list = []
            count = None
            frete_valor = frete_prazo = frete_metodo = ''
            try:
                driver, html, err = ensure_driver_and_get(driver, url, headless=headless, retries=2)
                if html is None:
                    status = 'erro'; erro = f'{err!r}'
                else:
                    soup = BeautifulSoup(html, 'html.parser')
                    parser = pick_parser(url)
                    cep = shipping_cep or CEP_DEFAULT
                    price, seller, rating, count, f_val, f_prazo, f_met, debug = parser(soup, html, driver=driver, cep=cep)
                    price_debug_list.extend(debug)
                    price_num = br_to_float(price)
                    if not price:
                        all_text = soup.get_text(' ')
                        regex_price = CURRENCY_RE.findall(all_text)
                        if regex_price:
                            nums = sorted([(p, br_to_float(p)) for p in set(regex_price)], key=lambda x: (x[1] if x[1] is not None else 1e12))
                            if nums and nums[0][1] is not None:
                                price = norm_price_str(nums[0][0]); price_num = nums[0][1]; price_debug_list.append('fallback:regex-min')
                    seller = clean_seller(seller)
                    if not rating:
                        m_r = RATING_RE.search(soup.get_text(' '))
                        if m_r:
                            val = m_r.group(1).replace('.', ',')
                            rating = f"{val} de 5"
                    frete_valor, frete_prazo, frete_metodo = f_val, f_prazo, f_met
            except Exception as e:
                status = 'erro'; erro = f'{type(e).__name__}: {e}'
            finished = datetime.now()
            results.append({
                'produto': produto,
                'url': url,
                'fonte_coluna': fonte,
                'preco': price,
                'preco_num': price_num,
                'fornecedor': seller,
                'avaliacao': rating,
                'avaliacoes_qtd': count,
                'frete_valor': frete_valor,
                'frete_prazo': frete_prazo,
                'frete_metodo': frete_metodo,
                'data_coleta': finished.strftime('%Y-%m-%d %H:%M:%S'),
                'duracao_s': round((finished-started).total_seconds(), 2),
                'status': status,
                'erro': erro,
                'preco_debug': '; '.join(price_debug_list[:10]),
                'preco_fontes': 'jsonld>dom>regex'
            })
            time.sleep(0.7 + random.random()*1.0)
    finally:
        try:
            if driver is not None:
                driver.quit()
        except Exception:
            pass
    out = pd.DataFrame(results)
    out.to_excel(output_excel, index=False)
    out.to_csv(output_csv, index=False, encoding='utf-8-sig')
    return out, output_excel, output_csv

if __name__ == '__main__':
    in_path = sys.argv[1] if len(sys.argv) > 1 else 'produtos.xlsx'
    out_xlsx = sys.argv[2] if len(sys.argv) > 2 else 'produtos_scrape.xlsx'
    out_csv = sys.argv[3] if len(sys.argv) > 3 else 'produtos_scrape.csv'
    headless = True
    if len(sys.argv) > 4:
        headless = sys.argv[4].lower() in ('1','true','yes','y')
    # CEP: usa o fixo por padrão; permite sobrescrever via arg 5
    shipping_cep = CEP_DEFAULT
    if len(sys.argv) > 5 and sys.argv[5]:
        shipping_cep = sys.argv[5]
    df, xlsx, csv = scrape_products(in_path, out_xlsx, out_csv, headless=headless, shipping_cep=shipping_cep)
    print(f'CEP usado: {shipping_cep}')
    print(f'Linhas coletadas: {len(df)}')
    print(f'Arquivo Excel: {xlsx}')
    print(f'Arquivo CSV: {csv}')

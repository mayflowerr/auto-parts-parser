import asyncio
import random
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

from patchright.async_api import async_playwright
from db import DB

DB_PATH = Path("partsgeek.sqlite3")

CATALOG_URL = "https://www.partsgeek.com/catalog/"

MAX_WORKERS = 5

NAV_TIMEOUT_MS = 20000
READY_SEL_TIMEOUT_MS = 7000
FALLBACK_LOAD_TIMEOUT_MS = 4000

X_CATALOG_LINKS = '//*[@id="whole"]/main/ul/li/a'
X_CATEGORY_PRODUCT_LINKS = '//*[@id="whole"]/main/ul/li/div[1]/a'
X_NEXT_PAGE = '//*[@id="whole"]//a[@rel="next" or contains(translate(normalize-space(.),"NEXT","next"),"next") or contains(., "След")]'

PRODUCT_READY_SEL = ".product-info-container"


# Утилиты
def canonicalize_url(url: str) -> str:
    parts = list(urlsplit(url))
    parts[4] = ""
    clean = urlunsplit(parts)
    if clean.endswith("/") and not parts[2] == "/":
        clean = clean.rstrip("/")
    return clean


async def configure_page(page):
    page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    page.set_default_timeout(READY_SEL_TIMEOUT_MS)


async def goto(page, url: str, *, ready_selector: str | None = None,
               sleep_after_ms: tuple[int, int] = (200, 400)):
    print(f"[i] → {url}")
    await page.goto(url, wait_until="load")  # ключевая смена: БЕЗ networkidle

    if ready_selector:
        try:
            await page.wait_for_selector(ready_selector, timeout=READY_SEL_TIMEOUT_MS)
        except Exception:
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=FALLBACK_LOAD_TIMEOUT_MS)
            except Exception:
                pass

    await page.wait_for_timeout(random.randint(*sleep_after_ms))


async def extract_links(page, xpath: str):
    nodes = await page.query_selector_all(f"xpath={xpath}")
    out = []
    for n in nodes:
        href = await n.get_attribute("href")
        if not href:
            continue
        text = (await n.inner_text()) or ""
        out.append((href.strip(), text.strip()))
    return out


async def find_next_page(page):
    el = await page.query_selector(f"xpath={X_NEXT_PAGE}")
    if not el:
        return None
    href = await el.get_attribute("href")
    return href.strip() if href else None


def parse_price_to_float(text: str | None):
    if not text:
        return None, None
    t = text.strip()
    m = re.search(r'([$\€£])?\s*([0-9][0-9\.,]*)', t)
    if not m:
        return None, None
    currency_symbol = (m.group(1) or "$").strip()
    raw = m.group(2).replace(",", "")
    try:
        return float(raw), {"$": "USD", "€": "EUR", "£": "GBP"}.get(currency_symbol, "USD")
    except Exception:
        return None, None


async def get_text(page, selector: str):
    el = await page.query_selector(selector)
    if not el:
        return None
    return (await el.inner_text() or "").strip()


async def extract_attr_block(page):
    attrs = {}
    rows = await page.query_selector_all(
        ".product-info-container .product-info .product-attributes > div, "
        ".product-info-container .product-info .product-attributes-red-bold > div, "
        ".product-info-container .product-info > div"
    )
    for row in rows:
        head_el = await row.query_selector(".product-attribute-heading")
        if not head_el:
            continue
        key = (await head_el.inner_text() or "").strip()
        key = key.rstrip(":").strip()
        val_el = await row.query_selector(".product-attribute-content")
        if val_el:
            val = (await val_el.inner_text() or "").strip()
        else:
            full = (await row.inner_text()) or ""
            full = full.strip()
            val = full
            if val.lower().startswith((key + ":").lower()):
                val = val[len(key) + 1:].strip()
        if key:
            attrs[key] = val
    return attrs


async def click_fitment_show_more_if_present(page):
    btn = await page.query_selector(".fitment-container .applications-container button.btn")
    if btn:
        try:
            await btn.click()
            await page.wait_for_timeout(200)
        except Exception:
            pass


async def extract_fitment_table(page):
    await click_fitment_show_more_if_present(page)
    rows = await page.query_selector_all(".fitment-container .applications-container table tbody tr")
    res = []
    for r in rows:
        tds = await r.query_selector_all("td.application-content")
        vals = []
        for td in tds:
            vals.append(((await td.inner_text()) or "").strip())
        vehicle = vals[0] if len(vals) > 0 else None
        sub_model = vals[1] if len(vals) > 1 else None
        engine = vals[2] if len(vals) > 2 else None
        if any([vehicle, sub_model, engine]):
            res.append({"vehicle": vehicle, "sub_model": sub_model, "engine": engine})
    return res


async def extract_hidden_inputs(page):
    data = {}
    for name in ["prod_id", "app_id", "alt_sku", "part_number"]:
        el = await page.query_selector(f'form.product-form input[name="{name}"]')
        if el:
            data[name] = await el.get_attribute("value")
    return data


# Парсеры уровней
async def parse_catalog(db: DB, page, url: str):
    await goto(page, url, ready_selector=f"xpath={X_CATALOG_LINKS}")
    links = await extract_links(page, X_CATALOG_LINKS)
    print(f"[✓] Категорий найдено: {len(links)}")

    for href, name in links:
        abs_url = canonicalize_url(urljoin(url, href))
        db.insert_category(abs_url, name)
        db.upsert_queue(abs_url, "category")

    db.mark_done(url)


async def parse_category(db: DB, page, url: str):
    cat_counts = db.category_counts()
    print(f"[cat {cat_counts['done'] + 1}/{cat_counts['total']}] → {url}")

    page_num = 1
    cur_url = url
    total_found_in_category = 0

    while True:
        await goto(page, cur_url, ready_selector=f"xpath={X_CATEGORY_PRODUCT_LINKS}")
        prods = await extract_links(page, X_CATEGORY_PRODUCT_LINKS)
        total_found_in_category += len(prods)
        print(f"[✓] Товаров на странице {page_num}: {len(prods)} (accum: {total_found_in_category})")

        for href, title in prods:
            abs_url = canonicalize_url(urljoin(cur_url, href))
            db.insert_product_discovery(abs_url, title, url)
            db.upsert_queue(abs_url, "product")

        next_rel = await find_next_page(page)
        if not next_rel:
            break
        cur_url = canonicalize_url(urljoin(cur_url, next_rel))
        page_num += 1

    db.mark_done(url)

    cat_counts = db.category_counts()
    print(f"[cat {cat_counts['done']}/{cat_counts['total']} done] {url}")


async def parse_product(db: DB, page, url: str, *, category_url: str | None = None):
    if db.product_exists(url):
        db.mark_done(url)
        if category_url:
            pc = db.product_counts_for_category(category_url)
            print(f"[prod {pc['done']}/{pc['total']} already] {url}")
        return

    if category_url:
        pc = db.product_counts_for_category(category_url)
        print(f"[prod {pc['done'] + 1}/{pc['total']} in cat] → {url}")

    await goto(page, url, ready_selector=PRODUCT_READY_SEL, sleep_after_ms=(200, 400))

    try:
        await page.wait_for_selector(PRODUCT_READY_SEL, timeout=2000)
    except Exception:
        db.mark_error(url, "product_container_not_found")
        print(f"[prod error] container not found → skip {url}")
        return

    title = await get_text(page, ".product-info-container .product-title h1")
    raw_price = await get_text(page, ".product-info-container .product-offer .product-price")
    price, currency = parse_price_to_float(raw_price)

    attrs = await extract_attr_block(page)
    part_number = attrs.get("Part Number")
    brand = attrs.get("Brand")

    hidden = await extract_hidden_inputs(page)
    if not part_number:
        part_number = hidden.get("part_number") or hidden.get("alt_sku")

    stock_text = await get_text(page, ".product-info-container .product-stock")
    stock = None
    if stock_text:
        m = re.search(r'\((\d+)\)', stock_text)
        if m:
            try:
                stock = int(m.group(1))
            except Exception:
                pass

    fitment = await extract_fitment_table(page)

    if not title:
        db.mark_error(url, "title_missing")
        print(f"[prod error] title missing → skip {url}")
        return

    data = {
        "url": url,
        "title": title,
        "price": price,
        "currency": currency or "USD",
        "part_number": part_number,
        "brand": brand,
        "stock": stock,
        "prod_id": hidden.get("prod_id") if hidden else None,
        "app_id": hidden.get("app_id") if hidden else None,
        "alt_sku": hidden.get("alt_sku") if hidden else None,
        "category_url": category_url,
        "attrs": attrs,
        "fitment": fitment,
        "scraped_at": time.time(),
    }
    db.upsert_product(data)
    db.mark_done(url)

    if category_url:
        pc = db.product_counts_for_category(category_url)
        price_str = f"{price} {currency}" if price is not None and currency else "n/a"
        pn = part_number or (data["alt_sku"] if data.get("alt_sku") else None) or "n/a"
        print(f"[prod {pc['done']}/{pc['total']} saved] {title} — {pn} — {price_str}")


# Воркеры
async def worker(db: DB, page, wid: int):
    await configure_page(page)

    idle_rounds = 0
    while True:
        row = db.reserve_next()
        if not row:
            if db.count_pending() == 0:
                return
            idle_rounds += 1
            await asyncio.sleep(min(0.5 + idle_rounds * 0.1, 2.0))
            continue

        url, kind = row
        try:
            if kind == "catalog":
                await parse_catalog(db, page, url)
            elif kind == "category":
                await parse_category(db, page, url)
            elif kind == "product":
                cur = db.conn.execute(
                    "SELECT category_url FROM product_discovery WHERE url=?",
                    (url,)
                ).fetchone()
                category_url = cur["category_url"] if cur else None
                await parse_product(db, page, url, category_url=category_url)
            else:
                db.mark_error(url, f"Unknown kind: {kind}")
        except Exception as e:
            print(f"[!] Ошибка на {url}: {e}")
            db.mark_error(url, repr(e))
            await page.wait_for_timeout(800 + random.randint(0, 400))


async def main():
    db = DB(DB_PATH)
    db.seed_catalog(CATALOG_URL)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        pages = [await browser.new_page() for _ in range(max(1, MAX_WORKERS))]
        await asyncio.gather(*(configure_page(pg) for pg in pages))

        tasks = [asyncio.create_task(worker(db, page, i)) for i, page in enumerate(pages, start=1)]
        await asyncio.gather(*tasks)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

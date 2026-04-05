# scraper/page_parser.py
import asyncio
import unicodedata
import re
from patchright.async_api import Page


# ── Main extractor ─────────────────────────────────────────────────────────────

async def get_result_count(page: Page) -> int:
    """Extract total result count from h3"""
    try:
        h3 = page.locator("h3").first
        text = await h3.inner_text(timeout=5000)
        # "Hasil pencarian 1 - 20 dari 62 nama..."
        match = re.search(r'dari\s+([\d\.]+)', text)
        if match:
            return int(match.group(1).replace(".", "").replace(",", ""))
    except Exception:
        pass
    return 0


async def get_total_pages(page: Page, per_page: int = 20) -> int:
    total = await get_result_count(page)
    if total == 0:
        return 0
    return (total + per_page - 1) // per_page


async def is_empty_result(page: Page) -> bool:
    """Check if page shows no results"""
    try:
        content = await page.content()
        return (
            "Pencarian Tidak Ditemukan" in content or
            "Keywords must be at least 3" in content
        )
    except Exception:
        return True


async def extract_companies_from_page(page: Page) -> list[dict]:
    """
    Extract all company cards directly using Playwright locators.
    Based on exact HTML: div.cl0, div.cl1 containing company data.
    """
    companies = []

    # Wait for results to be present
    try:
        await page.wait_for_selector("div[class^='cl']", timeout=8000)
    except Exception:
        print("  [PARSER] No company cards found on page")
        return companies

    # Select all company card containers (cl0, cl1, cl2...)
    cards = page.locator("div[class^='cl'] > div")
    count = await cards.count()

    for i in range(count):
        card = cards.nth(i)
        try:
            company = await extract_single_card(card)
            if company:
                companies.append(company)
        except Exception as e:
            print(f"  [PARSER] Card {i} error: {e}")
            continue

    return companies


async def extract_single_card(card) -> dict | None:
    """Extract data from one company card element"""
    try:
        # ── Company name + data-id ─────────────────────────────────────────
        name_el = card.locator("strong.judul").first
        if not await name_el.count():
            return None

        data_id = await name_el.get_attribute("data-id") or ""
        name    = await name_el.inner_text()
        name    = normalize_text(name)

        if not data_id or not name:
            return None

        # ── Phone ──────────────────────────────────────────────────────────
        phone = ""
        phone_el = card.locator("div.telp")
        if await phone_el.count():
            phone = await phone_el.first.inner_text()
            phone = normalize_phone(phone)

        # ── Address ────────────────────────────────────────────────────────
        address = ""
        address_el = card.locator("div.alamat")
        if await address_el.count():
            address = await address_el.first.inner_text()
            address = normalize_text(address)

        # ── City + Province ────────────────────────────────────────────────
        city, province = "", ""
        kabpro_el = card.locator("div.kabpro")
        if await kabpro_el.count():
            kabpro = await kabpro_el.first.inner_text()
            city, _, province = kabpro.partition(",")
            city     = normalize_text(city)
            province = normalize_text(province)

        return {
            "data_id":      data_id.strip(),
            "name":         name,
            "phone":        phone,
            "address":      address,
            "city":         city,
            "province":     province,
            "company_type": extract_company_type(name),
        }

    except Exception as e:
        print(f"  [PARSER] Single card error: {e}")
        return None


# ── Pagination ─────────────────────────────────────────────────────────────────

async def get_pagination_links(page: Page) -> list[str]:
    """
    Get all pagination hrefs directly from anchor tags.
    These already have the captcha token embedded by the site's JS.
    """
    links = []
    try:
        await page.wait_for_selector("a.search-pagination", timeout=5000)
        anchors = page.locator("a.search-pagination")
        count   = await anchors.count()

        for i in range(count):
            href = await anchors.nth(i).get_attribute("href")
            if href:
                # Convert relative to absolute
                if href.startswith("?"):
                    href = f"https://ahu.go.id/pencarian/profil-pt/{href}"
                links.append(href)
    except Exception:
        pass
    return links


async def get_active_page_number(page: Page) -> int:
    """Get currently active page number from pagination"""
    try:
        active = page.locator("a.search-pagination.active")
        if await active.count():
            text = await active.first.inner_text()
            return int(text.strip())
    except Exception:
        pass
    return 1


# ── Normalization ──────────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)        # collapse whitespace
    text = re.sub(r'[^\w\s\-\.,&()]', '', text)  # remove junk punctuation
    return text


def normalize_phone(phone: str) -> str:
    digits = re.sub(r'\D', '', phone)
    if digits.startswith("62"):
        digits = "0" + digits[2:]
    return digits


def extract_company_type(name: str) -> str:
    name_up = name.upper().strip()
    types = [w
        ("PT TBK",    "Public Company (Tbk)"),
        ("PT PERSERO","State-Owned Company"),
        ("PT",        "Perseroan Terbatas"),
        ("CV",        "Commanditaire Vennootschap"),
        ("UD",        "Usaha Dagang"),
        ("PD",        "Perusahaan Daerah"),
        ("FIRMA",     "Firma"),
        ("KOPERASI",  "Koperasi"),
        ("YAYASAN",   "Yayasan"),
    ]
    for prefix, label in types:
        if name_up.startswith(prefix + " ") or name_up == prefix:
            return label
    return "Unknown"
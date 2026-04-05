import re
import unicodedata
from patchright.async_api import Page
from scraper.logger import get_logger

log = get_logger()


# Page state checks
async def is_empty_result(page: Page) -> bool:
    """
    Returns True if the page shows no results.
    Covers: empty search, token expired, geo-blocked, keyword too short.
    """
    try:
        content = await page.content()
        return (
            "Pencarian Tidak Ditemukan" in content or
            "Keywords must be at least 3" in content or
            "tidak ditemukan" in content.lower()
        )
    except Exception:
        return True


async def get_result_count(page: Page) -> int:
    """
    Extract total result count from h3 heading.
    Example h3 text: 'Hasil pencarian 1 - 20 dari 62 nama cari profil...'
    """
    try:
        h3 = page.locator("h3").first
        await h3.wait_for(timeout=8_000)
        text = await h3.inner_text()
        # Match 'dari 62' or 'dari 220.109'
        match = re.search(r'dari\s+([\d\.]+)', text)
        if match:
            raw = match.group(1).replace(".", "").replace(",", "")
            return int(raw)
    except Exception as e:
        log.debug(f"[PARSER] get_result_count error: {e}")
    return 0

async def get_total_pages(page: Page, per_page: int = 20) -> int:
    """Calculate total pages from result count"""
    total = await get_result_count(page)
    if total == 0:
        return 0
    return (total + per_page - 1) // per_page

# ── Company Extraction ─────────────────────────────────────────────────────────

async def extract_companies_from_page(page: Page) -> list[dict]:
    """
    Extract all company cards from current page using Playwright locators.

    HTML structure (confirmed from live page source):
    <div class="cl0">
      <div>
        <strong data-id="273002" class="judul beli_profile_lengkap">
          PT <span class="match">Aaa</span> Cipta Persada
        </strong>
        <div class="telp">02129043860</div>
        <div class="alamat">GD. OFFICE 8 LT. 18-A...</div>
        <div class="kabpro">Jakarta Selatan, DKI Jakarta</div>
      </div>
    </div>
    """
    companies = []

    try:
        # Wait for at least one card to appear
        await page.wait_for_selector("div[class^='cl'] > div", timeout=8_000)
    except Exception:
        log.warning("[PARSER] No company cards found — page may be empty or timed out")
        return companies

    # All company card wrappers (cl0, cl1, cl2...)
    cards = page.locator("div[class^='cl'] > div")
    count = await cards.count()
    log.debug(f"[PARSER] Found {count} cards on page")

    for i in range(count):
        try:
            company = await _extract_card(cards.nth(i))
            if company:
                companies.append(company)
        except Exception as e:
            log.debug(f"[PARSER] Card {i} extract error: {e}")

    return companies


async def _extract_card(card) -> dict | None:
    """Extract all fields from a single company card element"""
    try:
        # ── Name + data-id (NBRS ID) ──────────────────────────────────────────
        name_el = card.locator("strong.judul").first
        if not await name_el.count():
            return None

        data_id = (await name_el.get_attribute("data-id") or "").strip()
        # inner_text() gives clean text without HTML tags
        name    = (await name_el.inner_text()).strip()
        # Collapse any extra whitespace left by <span class="match"> tags
        name    = re.sub(r'\s+', ' ', name).strip()

        if not data_id or not name:
            return None

        # ── Phone ──────────────────────────────────────────────────────────────
        phone = ""
        phone_el = card.locator("div.telp")
        if await phone_el.count():
            phone = (await phone_el.first.inner_text()).strip()

        # ── Address ────────────────────────────────────────────────────────────
        address = ""
        address_el = card.locator("div.alamat")
        if await address_el.count():
            address = (await address_el.first.inner_text()).strip()

        # ── City + Province ────────────────────────────────────────────────────
        city, province = "", ""
        kabpro_el = card.locator("div.kabpro")
        if await kabpro_el.count():
            kabpro   = (await kabpro_el.first.inner_text()).strip()
            # Format: "Jakarta Selatan, DKI Jakarta"
            city, _, province = kabpro.partition(",")
            city     = city.strip()
            province = province.strip()

        return {
            "data_id":      data_id,
            "name":         normalize_text(name),
            "phone":        normalize_phone(phone),
            "address":      normalize_text(address),
            "city":         normalize_text(city),
            "province":     normalize_text(province),
            "company_type": extract_company_type(name),
        }

    except Exception as e:
        log.debug(f"[PARSER] _extract_card error: {e}")
        return None


# ── Normalization ──────────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    """
    Full text normalization:
    - Unicode NFKC (handles Indonesian special chars like é, ñ, etc.)
    - Strip + collapse whitespace
    - Remove control characters
    """
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.strip()
    text = re.sub(r'[\x00-\x1f\x7f]', '', text)   # remove control chars
    text = re.sub(r'\s+', ' ', text)                # collapse whitespace
    return text


def normalize_phone(phone: str) -> str:
    """
    Normalize Indonesian phone numbers:
    - Strip non-digits
    - Convert 62xxx → 0xxx (international → local format)
    """
    if not phone:
        return ""
    digits = re.sub(r'\D', '', phone)
    if digits.startswith("62") and len(digits) > 5:
        digits = "0" + digits[2:]
    return digits


def extract_company_type(name: str) -> str:
    """
    Identify Indonesian company type from name prefix.
    Ordered by specificity (PT TBK before PT, etc.)
    """
    name_up = name.upper().strip()

    type_map = [
        ("PT TBK",      "Public Company (Tbk)"),
        ("PT PERSERO",  "State-Owned Company (Persero)"),
        ("PT",          "Perseroan Terbatas"),
        ("CV",          "Commanditaire Vennootschap"),
        ("UD",          "Usaha Dagang"),
        ("PD",          "Perusahaan Daerah"),
        ("FIRMA",       "Firma"),
        ("NV",          "Naamloze Vennootschap"),
        ("KOPERASI",    "Koperasi"),
        ("YAYASAN",     "Yayasan"),
        ("PERKUMPULAN", "Perkumpulan"),
    ]

    for prefix, label in type_map:
        # Match prefix followed by space, or exact match
        if name_up.startswith(prefix + " ") or name_up == prefix:
            return label

    return "Unknown"

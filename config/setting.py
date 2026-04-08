# setting.py
import random
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR             = Path(__file__).resolve().parent.parent
PROXIES_PATH         = BASE_DIR / "config" / "proxies.txt"
STATE_FILE           = BASE_DIR / "config" / "proxy_state.json"
COMPLETED_KEYWORDS_FILE = BASE_DIR / "config" / "completed_keywords.json"  # NEW: Track completed keywords
AHU_COMPANIES_CSV    = BASE_DIR / "database" / "ahu_companies.csv"
KEYWORDS_CSV_FILE    = BASE_DIR / "database" / "keywords.csv"
LOG_PATH             = BASE_DIR / "logs" / "scraper.log"
DB_SCREENSHOTS_PATH  = BASE_DIR / "db_screenshots"

# Ensure directories exist
COMPLETED_KEYWORDS_FILE.parent.mkdir(parents=True, exist_ok=True)
Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(DB_SCREENSHOTS_PATH).mkdir(parents=True, exist_ok=True)

# ── Target ─────────────────────────────────────────────────────────────────────
BASE_URL         = "https://ahu.go.id/pencarian/profil-pt"
SEARCH_TYPE      = "perseroan"
MAX_COMPANIES    = 10_000
MAX_PAGES        = 50  # NEW: Maximum pages to scrape per keyword

# ── Browser ────────────────────────────────────────────────────────────────────
HEADLESS     = True
PAGE_TIMEOUT = 60_000
CONCURRENCY  = 1
COMPANY_BUFFER_SIZE = 50

# ── Delays in milliseconds (callable — fresh random value each call) ───────────
def DELAY_BETWEEN_PAGES()    -> int: return random.randint(1_000, 3_000)
def DELAY_BETWEEN_KEYWORDS() -> int: return random.randint(1_000, 3_000)
def DELAY_ON_RETRY()         -> int: return random.randint(10_000, 20_000)

# ── Retry ──────────────────────────────────────────────────────────────────────
MAX_RETRY = 3

# ── Context Rotation ───────────────────────────────────────────────────────────
# MAX_REQUESTS_PER_CONTEXT = 1000  # rotate after this many requests
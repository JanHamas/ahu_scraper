import random
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR             = Path(__file__).resolve().parent.parent
PROXIES_PATH         = BASE_DIR / "config" / "proxies.txt"
STATE_FILE           = BASE_DIR / "config" / "proxy_state.json"
DB_PATH              = BASE_DIR / "database" / "ahu_companies.db"
LOG_PATH             = BASE_DIR / "logs" / "scraper.log"
DB_SCREENSHOTS_PATH  = BASE_DIR / "db_screenshots"

# perseroan
# ── Target ─────────────────────────────────────────────────────────────────────
BASE_URL         = "https://ahu.go.id/pencarian/profil-pt"
SEARCH_TYPE      = "perseroan"
MAX_COMPANIES    = 10_000

# ── Browser ────────────────────────────────────────────────────────────────────
HEADLESS     = True
PAGE_TIMEOUT = 60_000
CONCURRENCY  = 3

# ── Delays in milliseconds (callable — fresh random value each call) ───────────
def DELAY_BETWEEN_PAGES()    -> int: return random.randint(1_000, 3_000)
def DELAY_BETWEEN_KEYWORDS() -> int: return random.randint(1_000, 3_000)
def DELAY_ON_RETRY()         -> int: return random.randint(10_000, 20_000)

# ── Retry ──────────────────────────────────────────────────────────────────────
MAX_RETRY = 3

# ── Context Rotation ───────────────────────────────────────────────────────────
# MAX_REQUESTS_PER_CONTEXT = 1000  # rotate after this many requests
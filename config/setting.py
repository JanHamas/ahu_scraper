import random
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).resolve().parent.parent
PROXIES_PATH = BASE_DIR / "config" / "proxies.txt"
DB_PATH      = BASE_DIR / "database" / "ahu_companies.db"
LOG_PATH     = BASE_DIR / "logs" / "scraper.log"

# ── Target ─────────────────────────────────────────────────────────────────────
BASE_URL        = "https://ahu.go.id/pencarian/profil-pt"
SEARCH_TYPE     = "perseroan"
MAX_COMPANIES   = 10_000      # stop after this many unique companies
RESULTS_PER_PAGE = 20
OVERFLOW_LIMIT  = 160         # if results > this → expand keyword to next level

# ── Browser ────────────────────────────────────────────────────────────────────
HEADLESS     = False          # keep False → better reCAPTCHA v3 score
PAGE_TIMEOUT = 60_000         # ms
CONCURRENCY  = 1              # parallel browser contexts (1 per proxy)

# ── Delays (seconds) ───────────────────────────────────────────────────────────
DELAY_BETWEEN_PAGES    = random.randint(3000, 7000)  # between pagination requests
DELAY_BETWEEN_KEYWORDS = random.randint(3000, 7000)  # after finishing one keyword
DELAY_ON_RETRY         = random.randint(3000, 7000) # after a failed request

# ── Retry ──────────────────────────────────────────────────────────────────────
MAX_RETRY = 3




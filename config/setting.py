import random

BASE_URL        = "https://ahu.go.id/pencarian/profil-pt"
PROXIES_PATH    = "config/proxies.txt"
LOG_PATH    = "config/proxies.txt"
CAPTCHA_API_KEY = "YOUR_2CAPTCHA_OR_CAPSOLVER_KEY"
CAPTCHA_SERVICE = "2captcha"   # or "capsolver"

HEADLESS        = False        # keep False → better reCAPTCHA score
PAGE_TIMEOUT    = 60_000
DELAY_ON_RETRY  = random.randint(3000, 7000) # ms
DELAY_BETWEEN_PAGES = random.randint(3000, 7000) # MS
MAX_RETRY       = 5
CONCURRENCY     = 2            # number of parallel browser contexts
MAX_COMPANIES   = 10_000
DB_PATH         = "database/ahu_companies.db"
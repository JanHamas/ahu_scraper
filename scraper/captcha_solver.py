import asyncio
import os
import time
from twocaptcha import TwoCaptcha
from dotenv import load_dotenv
from scraper.logger import get_logger

load_dotenv()

log = get_logger()

SITE_KEY     = "6LdvmHwsAAAAAEbQuvif9ubf1cfoHLkTXb859OTp"
PAGE_URL     = "https://ahu.go.id/pencarian/profil-pt"
ACTION       = "cari"
TOKEN_TTL    = 110   # seconds — token lives ~2 min, use 110s to be safe


class RecaptchaBypasser:
    """
    Handles reCAPTCHA v3 bypass for AHU via 2captcha API.

    Token reuse strategy:
    - Token is valid ~2 minutes (we use 110s safe window)
    - Same token reused across multiple keywords while still fresh
    - New token only requested when current one is expired or about to expire
    - On token expiry mid-pagination → mark as expired, next keyword gets fresh one
    """

    def __init__(self, page, proxy: list[str] | None = None):
        self.page      = page
        self.api_key   = os.getenv("2CAPTCHA_API_KEY")
        self.proxy     = proxy

        # Token pool — shared across keywords
        self._token      : str | None = None
        self._token_time : float      = 0.0   # time.time() when token was received

        if not self.api_key:
            log.error("[CAPTCHA] 2CAPTCHA_API_KEY not set in .env!")

    # ── Public API ─────────────────────────────────────────────────────────────

    async def get_token(self) -> str | None:
        """
        Return current token if still fresh, otherwise solve a new one.
        This is the main method — replaces get_fresh_token().
        """
        if self._is_token_valid():
            age = int(time.time() - self._token_time)
            log.info(f"[CAPTCHA] ♻️  Reusing token (age: {age}s / {TOKEN_TTL}s)")
            return self._token

        # Token expired or not yet obtained — solve fresh
        log.info("[CAPTCHA] Requesting fresh reCAPTCHA v3 token...")
        token = await self._solve_async()
        if token:
            self._token      = token
            self._token_time = time.time()
            log.info("[CAPTCHA] ✅ Token received and cached")
        else:
            log.error("[CAPTCHA] ❌ Failed to get token")
        return token

    def invalidate_token(self) -> None:
        """
        Call this when token expiry is detected mid-pagination.
        Forces fresh solve on next keyword.
        """
        log.warning("[CAPTCHA] Token manually invalidated")
        self._token      = None
        self._token_time = 0.0

    def token_age(self) -> int:
        """Return current token age in seconds"""
        if not self._token:
            return 999
        return int(time.time() - self._token_time)

    def _is_token_valid(self) -> bool:
        """Check if current token is still within safe usage window"""
        if not self._token:
            return False
        return (time.time() - self._token_time) < TOKEN_TTL

    async def inject_token(self, token: str) -> None:
        """
        Inject solved token into live page DOM:
        1. Hidden textarea (#g-recaptcha-response-100000)
        2. Current URL via history.pushState
        3. All pagination anchor hrefs
        """
        await self.page.evaluate("""(token) => {
            const textarea = document.getElementById('g-recaptcha-response-100000');
            if (textarea) textarea.value = token;

            const uri    = window.location.href.split('?');
            const params = Object.fromEntries(new URLSearchParams(uri[1] || ''));
            params['g-recaptcha-response'] = token;
            params['recaptcha-version']    = 3;
            history.pushState(
                null, '',
                uri[0] + '?' + new URLSearchParams(params).toString()
            );

            document.querySelectorAll('a.search-pagination').forEach(el => {
                const href = el.getAttribute('href') || '';
                const p    = Object.fromEntries(
                    new URLSearchParams(href.startsWith('?') ? href.substr(1) : href)
                );
                p['g-recaptcha-response'] = token;
                p['recaptcha-version']    = 3;
                el.setAttribute('href', '?' + new URLSearchParams(p).toString());
            });
        }""", token)
        log.debug(f"[CAPTCHA] Token injected (age: {self.token_age()}s)")

    async def verify_token_alive(self) -> bool:
        """
        Check if current page still has valid results.
        Invalidates token cache if expired detected.
        """
        try:
            content = await self.page.content()
            if "Pencarian Tidak Ditemukan" in content:
                log.warning("[CAPTCHA] Token expired — invalidating cache")
                self.invalidate_token()
                return False
            return True
        except Exception as e:
            log.error(f"[CAPTCHA] verify_token_alive error: {e}")
            return False

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _solve_async(self) -> str | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._solve_sync)

    def _solve_sync(self) -> str | None:
        solver = TwoCaptcha(self.api_key)
        proxy_dict = self._build_proxy_dict()
        try:
            kwargs = dict(
                sitekey = SITE_KEY,
                url     = PAGE_URL,
                action  = ACTION,
                version = "V3",
                score   = 0.7,
            )
            if proxy_dict:
                kwargs["proxy"] = proxy_dict
                log.debug(f"[CAPTCHA] Solving via proxy: {proxy_dict['uri'].split('@')[-1]}")
            else:
                log.debug("[CAPTCHA] Solving without proxy")

            result = solver.recaptcha(**kwargs)
            return result["code"]
        except Exception as e:
            log.error(f"[CAPTCHA] 2Captcha error: {str(e).split('—')[-1].strip()}")
            return None

    def _build_proxy_dict(self) -> dict | None:
        if not self.proxy:
            return None
        try:
            ip, port, user, pwd = self.proxy
            return {"type": "HTTPS", "uri": f"{user}:{pwd}@{ip}:{port}"}
        except (ValueError, TypeError) as e:
            log.warning(f"[CAPTCHA] Invalid proxy format: {e}")
            return None
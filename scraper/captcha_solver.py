# scraper/captcha_bypasser.py
import asyncio
import os
from twocaptcha import TwoCaptcha
from dotenv import load_dotenv
from scraper.logger import get_logger

load_dotenv()

log = get_logger()

# Confirmed from AHU page source (grecaptcha.execute call)
SITE_KEY = "6LdvmHwsAAAAAEbQuvif9ubf1cfoHLkTXb859OTp"
PAGE_URL  = "https://ahu.go.id/pencarian/profil-pt"
ACTION    = "cari"


class RecaptchaBypasser:
    """
    Handles reCAPTCHA v3 bypass for AHU via 2captcha API.

    Follows the official 2captcha example pattern:
    - Passes proxy to solver so token is validated from same IP
    - Uses version='V3' (capital V, matching 2captcha SDK)
    - Runs sync solver in thread pool to not block async event loop
    """

    def __init__(self, page, proxy: list[str] | None = None):
        """
        Args:
            page:  Playwright page object
            proxy: [ip, port, user, password] or None
        """
        self.page    = page
        self.api_key = os.getenv("2CAPTCHA_API_KEY")
        self.proxy   = proxy   # passed from worker so solver uses same proxy

        if not self.api_key:
            log.error("[CAPTCHA] 2CAPTCHA_API_KEY not set in .env!")

    # ── Public API ─────────────────────────────────────────────────────────────

    async def get_fresh_token(self) -> str | None:
        """
        Request a fresh reCAPTCHA v3 token from 2captcha.
        Call once before each keyword search.
        Token is valid ~2 minutes (covers ~8 pages at 10s/page).
        """
        log.info("[CAPTCHA] Requesting fresh reCAPTCHA v3 token...")
        token = await self._solve_async()
        if token:
            log.info("[CAPTCHA] ✅ Token received")
        else:
            log.error("[CAPTCHA] ❌ Failed to get token")
        return token

    async def inject_token(self, token: str) -> None:
        """
        Inject solved token into live page DOM:
        1. Hidden textarea field (#g-recaptcha-response-100000)
        2. Current page URL via history.pushState
        3. All pagination anchor hrefs (mirrors AHU's own grecaptcha.ready JS)
        """
        await self.page.evaluate("""(token) => {
            // 1. Hidden textarea
            const textarea = document.getElementById('g-recaptcha-response-100000');
            if (textarea) textarea.value = token;

            // 2. Update current URL
            const uri    = window.location.href.split('?');
            const params = Object.fromEntries(new URLSearchParams(uri[1] || ''));
            params['g-recaptcha-response'] = token;
            params['recaptcha-version']    = 3;
            history.pushState(
                null, '',
                uri[0] + '?' + new URLSearchParams(params).toString()
            );

            // 3. Update all pagination links (same as AHU's own JS does)
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
        log.debug("[CAPTCHA] Token injected into page DOM and pagination links")

    async def verify_token_alive(self) -> bool:
        """
        Check if current page still has valid results.
        Returns False when token expired (site returns empty result page).
        """
        try:
            content = await self.page.content()
            if "Pencarian Tidak Ditemukan" in content:
                log.warning("[CAPTCHA] Token expired — empty result page detected")
                return False
            return True
        except Exception as e:
            log.error(f"[CAPTCHA] verify_token_alive error: {e}")
            return False

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _solve_async(self) -> str | None:
        """Run blocking 2captcha call in thread pool — keeps event loop free"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._solve_sync)

    def _solve_sync(self) -> str | None:
        """
        Synchronous 2captcha solve — matches official 2captcha example pattern.
        Passes proxy so token is solved from the same IP the scraper uses.
        """
        solver = TwoCaptcha(self.api_key)

        # Build proxy dict matching 2captcha SDK format
        # Example from 2captcha docs: {'type': 'HTTPS', 'uri': 'user:pwd@ip:port'}
        proxy_dict = self._build_proxy_dict()

        try:
            kwargs = dict(
                sitekey = SITE_KEY,
                url     = PAGE_URL,
                action  = ACTION,
                version = "V3",    # capital V — matches 2captcha example exactly
                score   = 0.7,
            )

            # Only pass proxy if we have one
            if proxy_dict:
                kwargs["proxy"] = proxy_dict
                log.debug(f"[CAPTCHA] Solving via proxy: {proxy_dict['uri'].split('@')[-1]}")
            else:
                log.debug("[CAPTCHA] Solving without proxy")

            result = solver.recaptcha(**kwargs)
            return result["code"]

        except Exception as e:
            error_msg = str(e).split("—")[-1].strip()
            log.error(f"[CAPTCHA] 2Captcha error: {error_msg}")
            return None

    def _build_proxy_dict(self) -> dict | None:
        """
        Convert [ip, port, user, password] → 2captcha proxy dict format.
        Format: {'type': 'HTTPS', 'uri': 'username:password@ip:port'}
        """
        if not self.proxy:
            return None
        try:
            ip, port, user, pwd = self.proxy
            return {
                "type": "HTTPS",
                "uri":  f"{user}:{pwd}@{ip}:{port}",
            }
        except (ValueError, TypeError) as e:
            log.warning(f"[CAPTCHA] Invalid proxy format: {e}")
            return None

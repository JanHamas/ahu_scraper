import asyncio
from patchright.async_api import async_playwright, Browser, BrowserContext, Page

from config import setting
from scraper.logger import get_logger
from scraper.helper import (
    load_proxies,
    get_timezone_from_ip,
    get_proxy_public_ip,
    build_js_script,
    webrtc_ip_spoof_script,
)
from scraper.captcha_bypasser import RecaptchaBypasser
from scraper.keyword_generator import KeywordGenerator
from scraper.page_parser import (
    is_empty_result,
    get_result_count,
    get_total_pages,
    extract_companies_from_page,
)
from database.db import DBHandler

import fg_generator  # your existing fingerprint generator

log = get_logger()


# ── Keyword Scraper ────────────────────────────────────────────────────────────

async def scrape_keyword(
    page: Page,
    keyword: str,
    bypasser: RecaptchaBypasser,
    db: DBHandler,
) -> tuple[str, int]:
    """
    Scrape all result pages for one keyword using a single captcha token.

    Returns:
        (status, result_count)
        status: 'done' | 'overflow' | 'singleton' | 'empty' | 'failed'
    """
    # ── Step 1: Get fresh captcha token ───────────────────────────────────────
    token = await bypasser.get_fresh_token()
    if not token:
        log.error(f"[SCRAPER] No token for '{keyword}' — skipping")
        return "failed", 0

    # ── Step 2: Load page 1 ───────────────────────────────────────────────────
    url = (
        f"{setting.BASE_URL}"
        f"?nama={keyword}"
        f"&tipe={setting.SEARCH_TYPE}"
        f"&page=1"
        f"&g-recaptcha-response={token}"
        f"&recaptcha-version=3"
    )

    try:
        await page.goto(url, wait_until="load", timeout=setting.PAGE_TIMEOUT)
    except Exception as e:
        log.error(f"[SCRAPER] Page load failed for '{keyword}': {e}")
        return "failed", 0

    # ── Step 3: Inject token into live page DOM ────────────────────────────────
    await bypasser.inject_token(token)

    # ── Step 4: Check page state ──────────────────────────────────────────────
    if await is_empty_result(page):
        log.info(f"[SCRAPER] '{keyword}' → empty")
        return "empty", 0

    total = await get_result_count(page)
    log.info(f"[SCRAPER] '{keyword}' → {total} results")

    if total == 0:
        return "empty", 0

    if total == 1:
        log.info(f"[SCRAPER] '{keyword}' → singleton (discarding)")
        return "singleton", 1

    if total > setting.OVERFLOW_LIMIT:
        log.info(f"[SCRAPER] '{keyword}' → overflow ({total} > {setting.OVERFLOW_LIMIT})")
        return "overflow", total

    # ── Step 5: Scrape page 1 ─────────────────────────────────────────────────
    companies = await extract_companies_from_page(page)
    saved = 0
    for co in companies:
        result = db.upsert_company(co, keyword=keyword)
        if result:
            saved += 1

    log.info(f"[SCRAPER] '{keyword}' page 1 → {len(companies)} extracted, {saved} new")

    # ── Step 6: Paginate ──────────────────────────────────────────────────────
    total_pages = await get_total_pages(page)
    log.debug(f"[SCRAPER] '{keyword}' → {total_pages} pages total")

    for page_num in range(2, total_pages + 1):

        # Check target reached
        if db.get_total_companies() >= setting.MAX_COMPANIES:
            log.info(f"[SCRAPER] Target {setting.MAX_COMPANIES} reached — stopping pagination")
            break

        next_url = (
            f"{setting.BASE_URL}"
            f"?nama={keyword}"
            f"&tipe={setting.SEARCH_TYPE}"
            f"&page={page_num}"
            f"&g-recaptcha-response={token}"
            f"&recaptcha-version=3"
        )

        try:
            await page.goto(
                next_url,
                wait_until="load",
                timeout=setting.PAGE_TIMEOUT,
            )
        except Exception as e:
            log.warning(f"[SCRAPER] '{keyword}' page {page_num} load error: {e}")
            break

        # Token expiry check
        if not await bypasser.verify_token_alive():
            log.warning(f"[SCRAPER] '{keyword}' token expired at page {page_num}")
            break

        companies = await extract_companies_from_page(page)
        page_saved = 0
        for co in companies:
            result = db.upsert_company(co, keyword=keyword)
            if result:
                page_saved += 1

        log.info(
            f"[SCRAPER] '{keyword}' page {page_num}/{total_pages} "
            f"→ {len(companies)} extracted, {page_saved} new "
            f"| DB total: {db.get_total_companies()}"
        )

        await asyncio.sleep(setting.DELAY_BETWEEN_PAGES)

    return "done", total


# ── Worker ─────────────────────────────────────────────────────────────────────

async def worker(
    browser: Browser,
    proxy: list[str] | None,
    db: DBHandler,
    keyword_queue: asyncio.Queue,
    worker_id: int,
) -> None:
    """
    One worker = one browser context = one proxy.
    Pulls keywords from shared queue until empty or target reached.
    """
    log.info(f"[WORKER-{worker_id}] Starting")

    # ── Build browser context ──────────────────────────────────────────────────
    fingerprint = fg_generator.generate()
    script      = build_js_script(fingerprint)

    context_options: dict = {
        "no_viewport": True,
        "user_agent":  fingerprint["user_agent"],
        "extra_http_headers": {
            "Accept-Language": fingerprint["headers"]["Accept-Language"],
        },
    }

    if proxy:
        ip, port, user, pwd = proxy
        timezone        = get_timezone_from_ip(ip)
        proxy_public_ip = get_proxy_public_ip(ip, port, user, pwd)

        context_options["timezone_id"] = timezone
        context_options["proxy"] = {
            "server":   f"http://{ip}:{port}",
            "username": user,
            "password": pwd,
        }
    else:
        context_options["timezone_id"] = "Asia/Jakarta"
        proxy_public_ip = "127.0.0.1"
        log.warning(f"[WORKER-{worker_id}] No proxy — running on real IP")

    context: BrowserContext = await browser.new_context(**context_options)
    await context.add_init_script(script)
    await context.add_init_script(webrtc_ip_spoof_script(proxy_public_ip))

    page: Page = await context.new_page()
    bypasser   = RecaptchaBypasser(page)
    kg         = KeywordGenerator()

    # ── Process keyword queue ──────────────────────────────────────────────────
    try:
        while True:

            if db.get_total_companies() >= setting.MAX_COMPANIES:
                log.info(f"[WORKER-{worker_id}] Target reached — stopping")
                break

            try:
                keyword = keyword_queue.get_nowait()
            except asyncio.QueueEmpty:
                log.info(f"[WORKER-{worker_id}] Queue empty — stopping")
                break

            log.info(f"[WORKER-{worker_id}] Processing keyword: '{keyword}'")

            status, count = "failed", 0
            for attempt in range(1, setting.MAX_RETRY + 1):
                try:
                    status, count = await scrape_keyword(page, keyword, bypasser, db)
                    break  # success — exit retry loop
                except Exception as e:
                    log.warning(
                        f"[WORKER-{worker_id}] '{keyword}' attempt {attempt} error: {e}"
                    )
                    if attempt < setting.MAX_RETRY:
                        await asyncio.sleep(setting.DELAY_ON_RETRY)

            # ── Handle result status ───────────────────────────────────────────
            if status == "overflow":
                # Expand to next letter depth
                expanded = kg.expand(keyword)
                added    = db.add_expanded_keywords(expanded)
                # Push new keywords into live queue
                for kw in expanded:
                    await keyword_queue.put(kw)
                db.mark_keyword(keyword, "overflow", count)
                log.info(
                    f"[WORKER-{worker_id}] '{keyword}' overflow → "
                    f"expanded {len(expanded)} keywords ({added} new)"
                )

            elif status == "done":
                db.mark_keyword(keyword, "done", count)

            elif status in ("empty", "singleton"):
                db.mark_keyword(keyword, status, count)

            elif status == "failed":
                db.mark_keyword(keyword, "failed", count)
                # Requeue for another worker to retry later
                await keyword_queue.put(keyword)
                log.warning(f"[WORKER-{worker_id}] '{keyword}' failed — requeued")

            keyword_queue.task_done()
            db.print_stats()
            await asyncio.sleep(setting.DELAY_BETWEEN_KEYWORDS)

    finally:
        await context.close()
        log.info(f"[WORKER-{worker_id}] Context closed")


# ── Main Entry ─────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info("=" * 60)
    log.info("  AHU Company Scraper — Starting")
    log.info("=" * 60)

    # ── Init DB ────────────────────────────────────────────────────────────────
    db = DBHandler(setting.DB_PATH)

    # ── Seed keywords (only on first run) ─────────────────────────────────────
    pending = db.get_pending_keywords()
    if not pending:
        log.info("[MAIN] First run — seeding 3-letter keywords...")
        kg       = KeywordGenerator()
        all_kws  = kg.generate_3letter()
        db.seed_keywords(all_kws)
        pending  = db.get_pending_keywords()

    log.info(
        f"[MAIN] {len(pending)} keywords pending | "
        f"{db.get_total_companies()} companies in DB"
    )

    if db.get_total_companies() >= setting.MAX_COMPANIES:
        log.info(f"[MAIN] Target {setting.MAX_COMPANIES} already reached — nothing to do")
        db.close()
        return

    # ── Build keyword queue ────────────────────────────────────────────────────
    keyword_queue: asyncio.Queue = asyncio.Queue()
    for kw in pending:
        await keyword_queue.put(kw)

    # ── Load proxies ───────────────────────────────────────────────────────────
    proxies = load_proxies(setting.PROXIES_PATH)
    if not proxies:
        log.warning("[MAIN] No proxies found — single worker without proxy")
        proxies = [None]

    n_workers = min(setting.CONCURRENCY, max(len(proxies), 1))

    # ── Launch browser + workers ───────────────────────────────────────────────
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=setting.HEADLESS)
        log.info(f"[MAIN] Browser launched | workers: {n_workers}")

        tasks = [
            worker(
                browser,
                proxies[i % len(proxies)] if proxies[0] else None,
                db,
                keyword_queue,
                worker_id=i + 1,
            )
            for i in range(n_workers)
        ]

        await asyncio.gather(*tasks)

        await browser.close()

    db.print_stats()
    log.info(f"[MAIN] ✅ Done — {db.get_total_companies()} unique companies scraped")
    db.close()
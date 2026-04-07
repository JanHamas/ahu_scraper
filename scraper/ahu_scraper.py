from scraper.logger import get_logger
from config import setting
from database.db import DBHandler
from .keyword_generator import KeywordGenerator
import asyncio
from .helper import(
    create_context,
    load_proxies,
    take_screenshot
)
from .page_parser import(
    is_empty_result,
    get_result_count,
    extract_companies_from_page,
    get_total_pages

)
from .captcha_solver import RecaptchaBypasser
from patchright.async_api import async_playwright, Browser, Page


log = get_logger()
def _build_url(keyword: str, page_num: int, token: str) -> str:
    """
    Build AHU search URL — exact parameter order from live browser:
    ?nama=X&tipe=perseroan&page=N&g-recaptcha-response=TOKEN&recaptcha-version=3
    """
    return (
        f"{setting.BASE_URL}"
        f"?nama={keyword}"
        f"&tipe={setting.SEARCH_TYPE}"
        f"&page={page_num}"
        f"&g-recaptcha-response={token}"
        f"&recaptcha-version=3"
    )

async def scrape_keyword(
    page: Page,
    keyword: str,
    bypasser: RecaptchaBypasser,
    db: DBHandler,
) -> tuple[str, int]:

    # ── Step 1: Get token (reused if still fresh, new if expired) ─────────────
    log.info(f"[SCRAPER] '{keyword}' — getting token...")
    token = await bypasser.get_token()
    if not token:
        log.error(f"[SCRAPER] No token for '{keyword}' — skipping")
        return "failed", 0

    # ── Step 2: Navigate directly to page 1 with token ────────────────────────
    try:
        await page.goto(
            _build_url(keyword, 1, token),
            wait_until="load",
            timeout=setting.PAGE_TIMEOUT,
        )
    except Exception as e:
        log.error(f"[SCRAPER] Page load failed for '{keyword}': {e}")
        return "failed", 0

    # ── Step 3: Inject token into DOM ─────────────────────────────────────────
    await bypasser.inject_token(token)

    # ── Step 4: Check page state ──────────────────────────────────────────────
    if await is_empty_result(page):
        log.info(f"[SCRAPER] '{keyword}' → empty")
        await take_screenshot(page, setting.DB_SCREENSHOTS_PATH, "empty_result")
        return "empty", 0

    total = await get_result_count(page)
    log.info(f"[SCRAPER] '{keyword}' → {total} results")

    if total == 0:    return "empty", 0
    if total == 1:
        log.info(f"[SCRAPER] '{keyword}' → singleton (discarding)")
        await take_screenshot(page, setting.DB_SCREENSHOTS_PATH, "singleton_result")
        return "singleton", 1

    # ── Step 5: Scrape page 1 ─────────────────────────────────────────────────
    companies = await extract_companies_from_page(page)
    saved = 0
    for co in companies:
        if db.upsert_company(co, keyword=keyword):
            saved += 1
    log.info(f"[SCRAPER] '{keyword}' page 1 → {len(companies)} extracted, {saved} new")

    # ── Step 6: Paginate ──────────────────────────────────────────────────────
    total_pages = await get_total_pages(page)
    log.debug(f"[SCRAPER] '{keyword}' → {total_pages} pages total")

    for page_num in range(2, total_pages + 1):

        if db.get_total_companies() >= setting.MAX_COMPANIES:
            log.info(f"[SCRAPER] Target {setting.MAX_COMPANIES} reached — stopping")
            break

        try:
            await page.goto(
                _build_url(keyword, page_num, token),
                wait_until="load",
                timeout=setting.PAGE_TIMEOUT,
            )
        except Exception as e:
            log.warning(f"[SCRAPER] '{keyword}' page {page_num} load error: {e}")
            break

        if not await bypasser.verify_token_alive():
            log.warning(f"[SCRAPER] '{keyword}' token expired at page {page_num}")
            break

        companies = await extract_companies_from_page(page)
        page_saved = 0
        for co in companies:
            if db.upsert_company(co, keyword=keyword):
                page_saved += 1

        log.info(
            f"[SCRAPER] '{keyword}' page {page_num}/{total_pages} "
            f"→ {len(companies)} extracted, {page_saved} new "
            f"| DB total: {db.get_total_companies()}"
        )
        # await page.wait_for_timeout(setting.DELAY_BETWEEN_PAGES())
        content = await page.content()
        if "Pencarian Tidak Ditemukan" in content:
            await take_screenshot(page, setting.DB_SCREENSHOTS_PATH, "last_page")
            break

    return "done", total

# Worker
async def worker(
        browser: Browser,
        proxy: list[str],
        db: DBHandler,
        keyword_queue: asyncio.Queue,
        worker_id: int
):
    """
    One worker = one browser context = one proxy if multiples available otherwise reuee same.
    Pulls keywrods from shared queue until empty to target reached.
    """
    log.info(f"[WORKER-{worker_id}] Starting")

    context = await create_context(browser, proxy)
    page = await context.new_page()
    bypasser = RecaptchaBypasser(page=page, proxy=proxy)

    # Process keyword queue
    try:
        while True:

            if db.get_total_companies() >= setting.MAX_COMPANIES:
                log.info(f"[WORKER-{worker_id}] Target reached - stopping")
                break

            try:
                keyword = keyword_queue.get_nowait()
            except asyncio.QueueEmpty:
                log.info(f"[WORKER-{worker_id}] Queue empty - stopping")
                break

            log.info(f"[WORKER-{worker_id}] Processing keyword: '{keyword}'")


            status, count = "failed", 0
            for attempt in range(1, setting.MAX_RETRY + 1):
                try:
                    status, count = await scrape_keyword(page, keyword, bypasser, db)
                    break # success - exit retry loop
                except Exception as e:
                    log.warning(
                        f"[WORKER-{worker_id}] '{keyword}' '{attempt}' error: {e}"
                    )
                    if attempt < setting.MAX_RETRY:
                        await page.wait_for_timeout(setting.DELAY_ON_RETRY)
            
            # handle result status
            if status == 'done':
                db.mark_keyword(keyword, "done", count)
            
            elif status in ("empty", "singleton"):
                db.mark_keyword(keyword, status, count)

            elif status == "failed":
                db.mark_keyword(keyword, "failed", count)
                # Requeue for another worker to retry later
                keyword_queue.put_nowait(keyword)
                log.warning(f"[WORKER-{worker_id}] '{keyword}' failed - requeued")
            
            keyword_queue.task_done()
            db.print_stats()

    finally:
        await context.close()
        log.info(f"[WORKER-{worker_id}] Context closed")



# Main Entry
async def main() -> None:
    log.info("\n \n \n")
    log.info("=" * 60)
    log.info("  AHU Company Scraper - Starting")
    log.info("=" * 60)

    # Init DB
    db = DBHandler(setting.DB_PATH)

    # Seed keywords (only of first run)
    pending = db.get_pending_keywords()
    if not pending:
        log.info("[MAIN] First run - seeding 3-letter keywords...")
        kg = KeywordGenerator()
        all_kws = kg.generate_3letter()
        db.seed_keywords(all_kws)
        pending = db.get_pending_keywords()

    log.info(
        f"[MAIN] {len(pending)} keywords pending !"
        f"{db.get_total_companies()} companies in DB"
    )

    if db.get_total_companies() >= setting.MAX_COMPANIES:
        log.info(f"[MAIN] Traget {setting.MAX_COMPANIES} already reached - nothing to do")
        db.close()
        return
    
    # Build keyword queue
    keyword_queue: asyncio.Queue = asyncio.Queue()
    for kw in pending:
        await keyword_queue.put(kw)

    # Load proxies
    proxies = load_proxies(setting.PROXIES_PATH)
    if not proxies:
        log.error(f"[MAIN] No proxies found - without proxies scraper not working.")
        return
    
    n_workers = setting.CONCURRENCY

    # Launch browser + worker
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=setting.HEADLESS)
        log.info(f"[MAIN] Browser launched | workers: {n_workers}")

        tasks = [
            worker(
                browser,
                proxies[i % len(proxies)],
                db,
                keyword_queue,
                worker_id = i + 1,
            )
            for i in range(n_workers)
        ]

        await asyncio.gather(*tasks)

        await browser.close()
    
    db.print_stats()
    log.info(f"[MAIN] Done - {db.get_total_companies()} unique companies scraped")
    db.close()

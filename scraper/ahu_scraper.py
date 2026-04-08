# scraper/worker.py - Complete corrected version
from scraper.logger import get_logger
from config import setting
import asyncio
from .helper import(
    create_context,
    load_keywords,
    load_proxies,
    company_writer,
    load_existing_nbrs_ids,
    take_screenshot,
    mark_keyword_complete
)
from .page_parser import extract_page_details
from .captcha_solver import RecaptchaBypasser
from patchright.async_api import async_playwright, Browser


log = get_logger()

# Worker
async def worker(
        browser: Browser,
        proxy: list[str],
        keyword_queue: asyncio.Queue,
        company_queue: asyncio.Queue,
        collected_nbrs_ids: set,
        worker_id: int
):
    """
    One worker = one browser context = one proxy if multiple available otherwise reuse same.
    Pulls keywords from shared queue until empty or target reached.
    """
    log.info(f"[WORKER-{worker_id}] Starting")

    context = await create_context(browser, proxy)
    page = await context.new_page()
    bypasser = RecaptchaBypasser(page=page, proxy=proxy)

    # Pagination loop
    while True:
        # ── Step 1: Get keyword from queue ─────────────
        try:
            keyword = keyword_queue.get_nowait()
            keyword_queue.task_done()
        except asyncio.QueueEmpty:
            log.info(f"[WORKER-{worker_id}] keyword queue is empty, stopping.")
            log.info(f"[WORKER-{worker_id}] Context closed")
            await context.close()
            return

        log.info(f"[WORKER-{worker_id}] Processing keyword: '{keyword}'")
        
        # ── Step 2: Get token ─────────────────────────
        token = await bypasser.get_token()
        if not token:
            log.error(f"[WORKER-{worker_id}] No token for '{keyword}' — skipping")
            continue

        # ── Step 3: Navigate to page 1 with token ─────
        try:
            url = (
                f"{setting.BASE_URL}"
                f"?nama={keyword}"
                f"&tipe={setting.SEARCH_TYPE}"
                f"&g-recaptcha-response={token}"
                f"&recaptcha-version=3"
            )
            await page.goto(url, wait_until="load", timeout=setting.PAGE_TIMEOUT)

            # Inject token into DOM
            await bypasser.inject_token(token)
            
            # Extract all companies from page 1
            await extract_page_details(page, company_queue, keyword, collected_nbrs_ids)
            
        except Exception as e:
            log.error(f"[WORKER-{worker_id}] Page 1 load failed for '{keyword}': {e}")
            continue

        # ── Step 4: Pagination through remaining pages ──
        page_num = 2
        max_pages = getattr(setting, 'MAX_PAGES', 50)
        found_no_results = False
        
        while page_num <= max_pages:
            # Check if token is still valid, refresh if needed
            if not await bypasser.verify_token_alive():
                log.warning(f"[WORKER-{worker_id}] Token expired for '{keyword}' at page {page_num}")
                token = await bypasser.get_token()
                if not token:
                    log.error(f"[WORKER-{worker_id}] Failed to refresh token for '{keyword}'")
                    break

            try:
                url = (
                    f"{setting.BASE_URL}"
                    f"?nama={keyword}"
                    f"&tipe={setting.SEARCH_TYPE}"
                    f"&page={page_num}"
                    f"&g-recaptcha-response={token}"
                    f"&recaptcha-version=3"
                )
                await page.goto(url, wait_until="load", timeout=setting.PAGE_TIMEOUT)
                
                # Inject fresh token
                await bypasser.inject_token(token)
                
                # Check if no results found
                content = await page.content()
                if "Pencarian Tidak Ditemukan" in content:
                    log.info(f"[WORKER-{worker_id}] No more results for '{keyword}' at page {page_num}")
                    found_no_results = True
                    break
                
                # Extract companies from current page
                await extract_page_details(page, company_queue, keyword, collected_nbrs_ids)
                
                page_num += 1
                
            except Exception as e:
                log.warning(f"[WORKER-{worker_id}] Page {page_num} load error for '{keyword}': {e}")
                break
        
        # Mark keyword as complete if we found the end
        if found_no_results:
            mark_keyword_complete(keyword)
            log.info(f"[WORKER-{worker_id}] Keyword '{keyword}' marked as complete")
        
        log.info(f"[WORKER-{worker_id}] Finished processing keyword: '{keyword}' (Total unique: {len(collected_nbrs_ids)})")

    log.info(f"[WORKER-{worker_id}] Shutting down")


# Main Entry
async def main() -> None:
    log.info("\n \n \n")
    log.info("=" * 60)
    log.info("  AHU Company Scraper - Starting")
    log.info("=" * 60)

    # Load existing NBRs IDs to avoid duplicates
    collected_nbrs_ids = load_existing_nbrs_ids()
    log.info(f"[MAIN] Loaded {len(collected_nbrs_ids)} existing NBRs IDs from CSV")

    # Build keyword queue & companies queue
    keyword_queue: asyncio.Queue = asyncio.Queue()
    company_queue: asyncio.Queue = asyncio.Queue()
    
    # Start company writer task
    writer_task = asyncio.create_task(company_writer(company_queue))
    
    # Load all keywords that are not processed
    await load_keywords(keyword_queue, collected_nbrs_ids)
    
    if keyword_queue.empty():
        log.warning("[MAIN] No keywords to process. Exiting.")
        writer_task.cancel()
        return

    # Load proxies
    proxies = load_proxies(setting.PROXIES_PATH)
    if not proxies:
        log.error("[MAIN] No proxies found - scraper cannot work without proxies.")
        writer_task.cancel()
        return
    
    n_workers = setting.CONCURRENCY
    log.info(f"[MAIN] Starting with {n_workers} workers")

    # Launch browser + workers
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=setting.HEADLESS)
        log.info("[MAIN] Browser launched")

        tasks = [
            worker(
                browser,
                proxies[i % len(proxies)],
                keyword_queue,
                company_queue,
                collected_nbrs_ids,
                worker_id=i + 1,
            )
            for i in range(n_workers)
        ]

        await asyncio.gather(*tasks)
        await browser.close()
        
        # Signal writer to stop and flush remaining data
        await company_queue.put(None)
        await writer_task
        
        log.info(f"[MAIN] Scraping completed. Total unique companies collected: {len(collected_nbrs_ids)}")
        log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
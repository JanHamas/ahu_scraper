from scraper.logger import get_logger
from config import setting
from database.db import DBHandler
from keyword_generator import KeywordGenerator
import asyncio
from helper import(
    load_proxies,
    build_js_script,
    get_timezone_from_ip,
    get_proxy_public_ip,
    webrtc_ip_spoof_script
)
import fg_generator
from patchright.async_api import async_playwright, Browser


log = get_logger()


# Worker
async def worker(
        browser: Browser,
        proxy: list[str],
        db: DBHandler,
        keyword_queue: asyncio.Queue,
        worker_id: int
        
):
    """
    One worker = one browser context = one proxy if multiples available otherwise resue same.
    Pulls keywrods from shared queue until empty to target reached.
    """
    log.info(f"[WORKER-{worker_id}] Starting")

    # Build browser context
    fingerprint = fg_generator.generate()
    script = build_js_script(fingerprint)

    ip, port, user, pwd = proxy
    timezone = get_timezone_from_ip(ip)
    proxy_public_ip = get_proxy_public_ip(ip)

    context_options = {
        "no_viewport": True,
        "user_agent": fingerprint["user_agent"],
        "timezone_id": timezone,
        "proxy":  {
            "server":   f"http://{ip}:{port}",
            "username": user,
            "password": pwd,
        },
        "extra_http_headers": {
            "Accept-Language": fingerprint["headers"]["Accept-Language"]
        }
    }

    context = await browser.new_context(**context_options)
    await context.add_init_script(script)
    await context.add_init_script(webrtc_ip_spoof_script(proxy_public_ip))

    page = await context.new_page()
    



# Main Entry
async def main() -> None:
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
    keyword_queue: asyncio.Queue = asyncio.Queue
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

# scraper/page_parser.py (updated with duplicate checking)
from patchright.async_api import Page
import asyncio
from scraper.logger import get_logger

log = get_logger()

async def extract_page_details(page: Page, company_queue: asyncio.Queue, keyword: str, collected_nbrs_ids: set):
    """
    Extract company details from page and only add to queue if NBRs ID is not already collected.
    """
    companies = page.locator("#hasil_cari > div.cl0, #hasil_cari > div.cl1")
    count = await companies.count()
    
    new_companies_count = 0
    duplicate_count = 0

    for i in range(count):
        company = companies.nth(i)

        # company name
        name_el = company.locator("strong.judul")
        company_name = (await name_el.inner_text()).strip()

        # NBRS ID
        nbrs_id = await name_el.get_attribute("data-id")
        
        # Skip if this NBRs ID is already collected
        if nbrs_id in collected_nbrs_ids:
            duplicate_count += 1
            continue

        # company type (first word)
        company_type = company_name.split()[0] if company_name else ""
        company_name_clean = " ".join(company_name.split()[1:])

        # phone (optional)
        phone_el = company.locator("div.telp")
        phone = await phone_el.inner_text() if await phone_el.count() > 0 else ""

        # address
        address = await company.locator("div.alamat").inner_text()

        # city / province
        kabpro = await company.locator("div.kabpro").inner_text()

        # Add to collected set to prevent future duplicates
        collected_nbrs_ids.add(nbrs_id)
        new_companies_count += 1

        # push to queue
        await company_queue.put({
            "nbrs_id": nbrs_id,
            "company_type": company_type,
            "company_name": company_name_clean,
            "phone": phone,
            "address": f"{address}, {kabpro}",
            "keyword": keyword,
        })
    
    if duplicate_count > 0:
        log.debug(f"[PARSER] Skipped {duplicate_count} duplicate companies for keyword '{keyword}'")
    if new_companies_count > 0:
        log.debug(f"[PARSER] Extracted {new_companies_count} new companies for keyword '{keyword}'")
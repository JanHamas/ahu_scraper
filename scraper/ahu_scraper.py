from patchright.async_api import async_playwright
from config import setting
from aioconsole import ainput
from . import helper, fg_generator
import asyncio


async def worker(browser: object, proxy: list):
    proxy_ip = proxy[0]
    proxy_ip, port, user, pwd = proxy

    timezone    = helper.get_timezone_from_ip(proxy_ip)
    fingerprint = fg_generator.generate()
    script = helper.build_js_script(fingerprint)

    proxy_public_ip = helper.get_proxy_public_ip(proxy_ip, port, user, pwd)

    context_options: dict = {
            "timezone_id":   timezone,
            "no_viewport":   True,
            "user_agent": fingerprint["user_agent"],

            "extra_http_headers": {
                "Accept-Language": fingerprint["headers"]["Accept-Language"],
            },
            "proxy": {
                "server": f"http://{proxy_ip}:{port}",
                "username": user,
                "password": pwd
            },
        }

    context = await browser.new_context(
        **context_options
    )
    await context.add_init_script(script)
    await context.add_init_script(helper._webrtc_ip_spoof_script(proxy_public_ip))
    
    page = await context.new_page()
    await page.goto(setting.BASE_URL, wait_until="load", timeout=setting.PAGE_TIMEOUT)
    await ainput("Press enter")
    await context.close()



async def main():
    proxies = helper.load_proxies(setting.PROXIES_PATH)
    async with async_playwright() as pw:
        browser_options = {
            "headless": setting.HEADLESS
        }
        browser = await pw.chromium.launch(**browser_options)
        
        tasks = []
        for i in range(setting.CONCURRENCTY):
            print(proxies[i % len(proxies)])
            tasks.append(worker(browser, proxies[i % len(proxies)]))

        # gather all tasks 
        await asyncio.gather(*tasks)
    
    

        
        


from patchright.async_api import async_playwright
import config.setting as setting, asyncio
from aioconsole import ainput
from util import



async def worker(browser):
    proxy = "45.147.133.69:12323:14ac990c0bff4:cc22ef0dc0"
    proxy_ip, port, user, pwd = proxy.split(":")
    timezone    = helper.get_timezone_from_ip(proxy_ip)
    context_options: dict = {
            "timezone_id":   timezone,
            "no_viewport":   True,
            "user_agent": fingerprint["user_agent"],

            "extra_http_headers": {
                "Accept-Language": fingerprint["headers"]["Accept-Language"],
            },
        }
    if proxy:
            context_options["args"] += [
                f"--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE {proxy_ip}",
                "--proxy-bypass-list=<-loopback>",
            ]
            context_options["proxy"] = {"server": f"socks5://127.0.0.1:{local_port}"}

    context = await browser.new_context(
        **context_options
    )
    await context.add_init_script(script)
    context = await browser.new_context(**context_options)
    page = await context.new_page()
    await page.goto(setting.BASE_URL, wait_until="load", timeout=setting.PAGE_TIMEOUT)
    await ainput("Press enter")
async def main():
    async with async_playwright() as pw:
        browser_options = {
            "headless": setting.HEADLESS
        }
        browser = await pw.chromium.launch(**browser_options)
        
        tasks = []
        for _ in range(setting.CONCURRENCTY):
            tasks.append(worker(browser))

        # gather all tasks 
        await asyncio.gather(*tasks)
    
    

        
        


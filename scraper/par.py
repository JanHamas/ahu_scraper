from patchright.async_api import async_playwright, Page
from aioconsole import ainput


BASE_URL         = "https://ahu.go.id/pencarian/profil-pt"


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        ip, port, user, pwd = "45.147.133.69:12323:14ac990c0bff4:cc22ef0dc0".split(":")
        context_options = {
        "no_viewport": True,
        "proxy":  {
            "server":   f"http://{ip}:{port}",
            "username": user,
            "password": pwd,
        }
         }
        context = await browser.new_context(**context_options)
        page = await context.new_page()
        await page.goto(BASE_URL, wait_until="load")
        await ainput("Press enter")


import asyncio
asyncio.run(main())
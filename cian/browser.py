async def get_browser_context(playwright, user_data_dir: str, channel: str = "chrome"):
    """Инициализирует и возвращает контекст браузера с защитой от детекта."""
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=user_data_dir,
        headless=False,
        channel=channel,
        args=["--disable-blink-features=AutomationControlled"],
        viewport={"width": 1366, "height": 768},
    )
    return context

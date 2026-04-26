import random

# Типичные разрешения российских десктопов
_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1600, "height": 900},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1280, "height": 800},
    {"width": 1280, "height": 720},
]

_STEALTH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-infobars",
    "--disable-notifications",
    "--disable-popup-blocking",
    "--password-store=basic",
    "--use-mock-keychain",
]


async def get_browser_context(playwright, user_data_dir: str, channel: str = "chrome"):
    """Инициализирует и возвращает контекст браузера с защитой от детекта."""
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=user_data_dir,
        headless=False,
        channel=channel,
        args=_STEALTH_ARGS,
        viewport=random.choice(_VIEWPORTS),
        locale="ru-RU",
        timezone_id="Europe/Moscow",
    )
    return context

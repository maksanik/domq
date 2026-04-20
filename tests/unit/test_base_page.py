import logging
from unittest.mock import MagicMock, patch


from cian.pages.base_page import BasePage


async def test_random_sleep_default_bounds():
    slept = []

    async def fake_sleep(t):
        slept.append(t)

    with patch("cian.pages.base_page.asyncio.sleep", side_effect=fake_sleep):
        await BasePage.random_sleep()

    assert len(slept) == 1
    assert 0.8 <= slept[0] <= 2.0


async def test_random_sleep_custom_bounds():
    slept = []

    async def fake_sleep(t):
        slept.append(t)

    with patch("cian.pages.base_page.asyncio.sleep", side_effect=fake_sleep):
        await BasePage.random_sleep(a=1.0, b=1.5)

    assert len(slept) == 1
    assert 1.0 <= slept[0] <= 1.5


async def test_random_sleep_without_logger_does_not_crash():
    with patch("cian.pages.base_page.asyncio.sleep"):
        await BasePage.random_sleep(logger=None)


async def test_random_sleep_with_logger_calls_info():
    mock_logger = MagicMock(spec=logging.Logger)

    with patch("cian.pages.base_page.asyncio.sleep"):
        await BasePage.random_sleep(logger=mock_logger)

    mock_logger.info.assert_called_once()


async def test_random_sleep_tight_bounds_respected():
    slept = []

    async def fake_sleep(t):
        slept.append(t)

    with patch("cian.pages.base_page.asyncio.sleep", side_effect=fake_sleep):
        for _ in range(20):
            await BasePage.random_sleep(a=0.1, b=0.2)

    assert all(0.1 <= t <= 0.2 for t in slept)

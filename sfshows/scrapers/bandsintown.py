from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn

from sfshows.config import Config
from sfshows.console import console
from sfshows.scrapers import BaseScraper, RawEvent

SOURCE = "bandsintown"
EVENT_URL_PREFIX = "https://www.bandsintown.com/e/"

_INVALID_ARTIST_NAMES: frozenset[str] = frozenset({
    "Upcoming Concerts",
    "Unknown",
})


def _extract_event_id(event_url: str) -> str:
    """Extract the numeric event ID from a Bandsintown event URL."""
    path = urlparse(event_url).path
    m = re.search(r"/e/(\d+)", path)
    return m.group(1) if m else path.lstrip("/")


MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_date(card) -> Optional[date]:
    """Extract the event start date from a card element by scanning month/day text nodes."""
    texts = [d.get_text(strip=True) for d in card.find_all("div") if d.get_text(strip=True)]
    month_num: Optional[int] = None
    day_num: Optional[int] = None

    for text in texts:
        lower = text.lower()
        if lower in MONTH_MAP and month_num is None:
            month_num = MONTH_MAP[lower]
        elif re.fullmatch(r"\d{1,2}", text) and day_num is None and month_num is not None:
            day_num = int(text)

    if month_num is None or day_num is None:
        return None

    today = date.today()
    year = today.year if month_num >= today.month else today.year + 1
    try:
        return date(year, month_num, day_num)
    except ValueError:
        return None


def _build_jsonld_datetime_map(soup) -> dict[str, str]:
    """Extract event ID → ISO datetime from JSON-LD MusicEvent blocks on the page."""
    dt_map: dict[str, str] = {}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if item.get("@type") != "MusicEvent":
                continue
            url = item.get("url", "")
            start = item.get("startDate", "")
            if not url or not start:
                continue
            event_id = _extract_event_id(url)
            if event_id:
                dt_map[event_id] = start
    return dt_map


def _parse_events(html: str, venue_url: str) -> list[RawEvent]:
    """Parse rendered Bandsintown venue HTML and return a deduplicated list of RawEvents."""
    soup = BeautifulSoup(html, "lxml")
    events: list[RawEvent] = []
    seen_ids: set[str] = set()

    # Build event_id → startDate map from JSON-LD structured data (includes time)
    jsonld_map = _build_jsonld_datetime_map(soup)

    # Venue name from page h1 — strip any SVG title text (e.g. "Verified")
    h1 = soup.find("h1")
    if h1:
        for svg in h1.find_all("svg"):
            svg.decompose()
        venue_name = h1.get_text(strip=True)
    else:
        venue_name = "Unknown Venue"

    for a in soup.find_all("a", href=re.compile(r"https://www\.bandsintown\.com/e/")):
        event_url = a["href"].split("?")[0]
        event_id = _extract_event_id(event_url)

        if event_id in seen_ids:
            continue
        seen_ids.add(event_id)

        # Artist name is the text directly inside the <a> tag
        artist_name = a.get_text(strip=True)
        if not artist_name or artist_name in _INVALID_ARTIST_NAMES:
            continue

        # Prefer JSON-LD startDate (includes time); fall back to card date-text parsing
        if event_id in jsonld_map:
            event_datetime_str = jsonld_map[event_id]
        else:
            card = a.parent.parent
            event_date = _parse_date(card)
            event_datetime_str = (
                datetime.combine(event_date, datetime.min.time()).isoformat(timespec="seconds")
                if event_date else ""
            )

        events.append(
            RawEvent(
                event_id=event_id,
                artist_name=artist_name,
                venue_name=venue_name,
                venue_city="San Francisco, CA",
                event_datetime=event_datetime_str,
                ticket_url=event_url,
                source=SOURCE,
            )
        )

    return events


class BandsintownScraper(BaseScraper):
    def __init__(self, config: Config) -> None:
        """Initialize the scraper with the given app config."""
        self._config = config

    async def scrape(
        self,
        save_html: Optional[str] = None,
    ) -> list[RawEvent]:
        """Scrape all configured Bandsintown venue pages concurrently and return raw events.

        If save_html is provided, the rendered HTML of the last scraped page is
        written to that path (useful for debugging CSS selectors).
        """
        concurrency = self._config.venue_concurrency
        semaphore = asyncio.Semaphore(concurrency)

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=self._config.user_agent,
                locale="en-US",
            )

            async def scrape_one(venue_url: str) -> list[RawEvent]:
                async with semaphore:
                    page = await context.new_page()
                    try:
                        await page.goto(venue_url, wait_until="load", timeout=60_000)

                        # Wait for event links to appear (faster than a fixed sleep)
                        try:
                            await page.wait_for_selector(
                                'a[href*="/e/"]', timeout=5000
                            )
                        except Exception:
                            # No events found or slow page — give it a brief extra moment
                            await page.wait_for_timeout(1000)

                        # Expand all dates if "Show More Dates" button is present
                        try:
                            show_more = page.get_by_text("Show More Dates", exact=False)
                            if await show_more.count() > 0:
                                await show_more.first.click()
                                await page.wait_for_timeout(1500)
                        except Exception:
                            pass

                        html = await page.content()
                    finally:
                        await page.close()

                if save_html:
                    with open(save_html, "w", encoding="utf-8") as f:
                        f.write(html)

                return _parse_events(html, venue_url)

            coros = [scrape_one(url) for url in self._config.venues]
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                console=console,
            ) as progress:
                task_id = progress.add_task("Scraping venues", total=len(coros))

                async def wrapped(coro):
                    result = await coro
                    progress.advance(task_id)
                    return result

                results: list[list[RawEvent]] = await asyncio.gather(
                    *[wrapped(c) for c in coros]
                )

            await browser.close()

        all_events: list[RawEvent] = []
        for events in results:
            all_events.extend(events)

        console.print(
            f"  [dim]Scraped[/] [bold]{len(all_events)}[/] [dim]events across[/] "
            f"[bold]{len(self._config.venues)}[/] [dim]venue(s)[/]"
        )
        return all_events

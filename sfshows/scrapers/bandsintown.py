from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from sfshows.config import Config
from sfshows.scrapers import BaseScraper, RawEvent

SOURCE = "bandsintown"
EVENT_URL_PREFIX = "https://www.bandsintown.com/e/"


def _extract_event_id(event_url: str) -> str:
    path = urlparse(event_url).path
    m = re.search(r"/e/(\d+)", path)
    return m.group(1) if m else path.lstrip("/")


MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_date(card) -> Optional[date]:
    """Extract the start date from an event card by finding month/day text nodes."""
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


def _parse_events(html: str, date_from: date, date_to: date, venue_url: str) -> list[RawEvent]:
    soup = BeautifulSoup(html, "lxml")
    events: list[RawEvent] = []
    seen_ids: set[str] = set()

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
        artist_name = a.get_text(strip=True) or "Unknown"

        # Event card is 2 levels up from the <a> tag
        card = a.parent.parent

        # Parse date from month/day text in card
        event_date = _parse_date(card)

        # Date window filter
        if event_date:
            if event_date < date_from or event_date > date_to:
                continue
            event_datetime_str = datetime.combine(event_date, datetime.min.time()).isoformat(timespec="seconds")
        else:
            event_datetime_str = ""

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
        self._config = config

    async def scrape(
        self,
        date_from: date,
        date_to: date,
        save_html: Optional[str] = None,
    ) -> list[RawEvent]:
        all_events: list[RawEvent] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=self._config.user_agent,
                locale="en-US",
            )

            for venue_url in self._config.venues:
                print(f"[scraper:bandsintown] Loading {venue_url}")
                page = await context.new_page()

                await page.goto(venue_url, wait_until="load", timeout=60_000)
                await page.wait_for_timeout(3000)  # allow JS to render events

                # Expand all dates if "Show More Dates" button is present
                try:
                    show_more = page.get_by_text("Show More Dates", exact=False)
                    if await show_more.count() > 0:
                        print(f"[scraper:bandsintown] Clicking 'Show More Dates'...")
                        await show_more.first.click()
                        await page.wait_for_timeout(2000)
                except Exception:
                    pass

                html = await page.content()
                await page.close()

                if save_html:
                    with open(save_html, "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"[scraper:bandsintown] HTML saved to {save_html}")

                events = _parse_events(html, date_from, date_to, venue_url)
                print(f"[scraper:bandsintown] {venue_url.split('/')[-1]}: found {len(events)} events")
                all_events.extend(events)

            await browser.close()

        print(f"[scraper:bandsintown] Total: {len(all_events)} events across {len(self._config.venues)} venue(s)")
        return all_events

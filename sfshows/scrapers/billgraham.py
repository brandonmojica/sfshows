from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from sfshows.scrapers import BaseScraper, RawEvent

SOURCE = "billgraham"
LISTING_URL = "https://billgrahamcivic.com/event-listing/"
VENUE_NAME = "Bill Graham Civic Auditorium"
VENUE_CITY = "San Francisco, CA"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}


def _parse_date(content: str) -> Optional[str]:
    """Parse 'April 25, 2026 7:30pm' → ISO8601 '2026-04-25T19:30:00'."""
    content = content.strip()
    for fmt in ("%B %d, %Y %I:%M%p", "%B %d, %Y %I:%M %p"):
        try:
            return datetime.strptime(content, fmt).isoformat(timespec="seconds")
        except ValueError:
            continue
    return None


def _event_id_from_url(url: str) -> str:
    """Extract slug from https://billgrahamcivic.com/events/said-the-sky-260425."""
    return urlparse(url).path.rstrip("/").split("/")[-1]


def _parse_events(html: str, date_from: date, date_to: date) -> list[RawEvent]:
    soup = BeautifulSoup(html, "lxml")
    events: list[RawEvent] = []
    seen: set[str] = set()

    # Each event card contains an element with itemprop="startDate"
    for date_el in soup.find_all(itemprop="startDate"):
        content = date_el.get("content", "")
        event_datetime = _parse_date(content)
        if not event_datetime:
            continue

        try:
            event_date = datetime.fromisoformat(event_datetime).date()
        except ValueError:
            continue

        if not (date_from <= event_date <= date_to):
            continue

        # Walk up to the card container (2 levels up from date-show div)
        card = date_el.parent.parent

        # Artist name — h2.show-title
        title_el = card.find("h2", class_="show-title")
        if title_el is None:
            title_el = card.find(itemprop="name")
        artist_name = title_el.get_text(strip=True) if title_el else "Unknown"

        # Event detail URL — first <a href="/events/..."> in the card
        detail_link = card.find("a", href=re.compile(r"/events/"))
        event_url = detail_link["href"] if detail_link else None
        event_id = _event_id_from_url(event_url) if event_url else event_datetime

        if event_id in seen:
            continue
        seen.add(event_id)

        # Ticket URL — <a class="button"> inside div.event-data (not the ghost/more-info one)
        event_data = card.find("div", class_="event-data")
        ticket_url: Optional[str] = None
        if event_data:
            for a in event_data.find_all("a", class_="button"):
                classes = a.get("class", [])
                if "ghost" not in classes and "more-info" not in classes:
                    ticket_url = a.get("href")
                    break

        events.append(
            RawEvent(
                event_id=event_id,
                artist_name=artist_name,
                venue_name=VENUE_NAME,
                venue_city=VENUE_CITY,
                event_datetime=event_datetime,
                ticket_url=ticket_url,
                source=SOURCE,
            )
        )

    return events


class BillGrahamScraper(BaseScraper):
    async def scrape(
        self,
        date_from: date,
        date_to: date,
        save_html: Optional[str] = None,
    ) -> list[RawEvent]:
        print(f"[scraper:billgraham] Fetching {LISTING_URL}")
        with httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True) as client:
            resp = client.get(LISTING_URL)
            resp.raise_for_status()

        if save_html:
            with open(save_html, "w", encoding="utf-8") as f:
                f.write(resp.text)
            print(f"[scraper:billgraham] HTML saved to {save_html}")

        events = _parse_events(resp.text, date_from, date_to)
        print(f"[scraper:billgraham] Found {len(events)} events in date window")
        return events

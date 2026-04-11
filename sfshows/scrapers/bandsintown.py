from __future__ import annotations

import re
import time
from datetime import date, datetime
from typing import Optional
from urllib.parse import urlparse

import httpx

from sfshows.config import Config
from sfshows.scrapers import BaseScraper, RawEvent

SOURCE = "bandsintown"
EVENTS_API = "https://www.bandsintown.com/all-dates/fetch-next/upcomingEvents"


def _first_page_url(lat: float, lon: float) -> str:
    return f"{EVENTS_API}?page=1&longitude={lon}&latitude={lat}"


def _extract_event_id(event_url: str) -> str:
    """Extract numeric event ID from a Bandsintown event URL.
    e.g. https://www.bandsintown.com/e/108164990-artist-name -> '108164990'
    """
    path = urlparse(event_url).path
    m = re.search(r"/e/(\d+)", path)
    return m.group(1) if m else path.lstrip("/")


def _in_cities(location_text: str, cities: tuple[str, ...]) -> bool:
    """Return True if locationText contains any of the configured city strings."""
    loc = location_text.lower()
    return any(city.lower() in loc for city in cities)


class BandsintownScraper(BaseScraper):
    def __init__(self, config: Config) -> None:
        self._config = config
        self._headers = {
            "User-Agent": self._config.user_agent,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.bandsintown.com/",
        }

    async def scrape(  # async to satisfy BaseScraper ABC; httpx calls are sync internally
        self,
        date_from: date,
        date_to: date,
        save_html: Optional[str] = None,  # kept for CLI compat, unused
    ) -> list[RawEvent]:
        events: list[RawEvent] = []
        seen_ids: set[str] = set()
        url: Optional[str] = _first_page_url(self._config.latitude, self._config.longitude)
        past_window = False

        with httpx.Client(headers=self._headers, timeout=15, follow_redirects=True) as client:
            for page_num in range(1, self._config.max_pages + 1):
                if not url:
                    break

                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
                records = data.get("events", [])

                print(f"[scraper] Page {page_num}: {len(records)} records", end="")

                page_events = 0
                for record in records:
                    starts_at = record.get("startsAt", "")
                    if not starts_at:
                        continue

                    try:
                        event_dt = datetime.fromisoformat(starts_at)
                    except ValueError:
                        continue

                    event_date = event_dt.date()

                    # Once we've passed the date window, stop paginating
                    if event_date > date_to:
                        past_window = True
                        break

                    if event_date < date_from:
                        continue

                    location = record.get("locationText", "")
                    if not _in_cities(location, self._config.cities):
                        continue

                    event_url = record.get("eventUrl", "")
                    event_id = _extract_event_id(event_url) if event_url else starts_at
                    if event_id in seen_ids:
                        continue
                    seen_ids.add(event_id)

                    events.append(
                        RawEvent(
                            event_id=event_id,
                            artist_name=record.get("artistName", "Unknown"),
                            venue_name=record.get("venueName", "Unknown Venue"),
                            venue_city=location,
                            event_datetime=event_dt.isoformat(timespec="seconds"),
                            ticket_url=event_url.split("?")[0] if event_url else None,
                            source=SOURCE,
                        )
                    )
                    page_events += 1

                print(f", {page_events} matched")

                if past_window:
                    print(f"[scraper] Passed {date_to} — stopping pagination")
                    break

                url = data.get("urlForNextPageOfEvents")
                if not url:
                    break

                time.sleep(0.3)  # be polite

        print(f"[scraper] Total: {len(events)} SF events in date window")
        return events

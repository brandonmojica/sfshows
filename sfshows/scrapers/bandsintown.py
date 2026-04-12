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
BIT_BASE = "https://www.bandsintown.com"
ALL_DATES_API = f"{BIT_BASE}/all-dates/fetch-next/upcomingEvents"


def _venue_param(venue_url: str) -> str:
    """Extract API param from URL: 'https://.../v/10019956-the-midway' → '10019956-the-midway'."""
    path = urlparse(venue_url).path.rstrip("/")
    return path.split("/v/")[-1]


def _venue_slug(venue_url: str) -> str:
    """Human-readable slug: '10019956-the-midway' → 'the-midway'."""
    param = _venue_param(venue_url)
    # Strip leading numeric ID segment
    return re.sub(r"^\d+-", "", param)


def _venue_keywords(venue_url: str) -> list[str]:
    """Words 5+ chars from slug used for venueName matching: 'the-midway' → ['midway']."""
    slug = _venue_slug(venue_url)
    return [w for w in slug.split("-") if len(w) >= 5]


def _matches_venue(venue_name: str, keywords: list[str]) -> bool:
    """Return True if venueName contains all venue keywords (case-insensitive)."""
    name_lower = venue_name.lower()
    return all(kw in name_lower for kw in keywords) if keywords else True


def _extract_event_id(event_url: str) -> str:
    path = urlparse(event_url).path
    m = re.search(r"/e/(\d+)", path)
    return m.group(1) if m else path.lstrip("/")


class BandsintownScraper(BaseScraper):
    def __init__(self, config: Config) -> None:
        self._config = config
        self._headers = {
            "User-Agent": self._config.user_agent,
            "Accept": "application/json, text/plain, */*",
            "Referer": BIT_BASE + "/",
        }

    async def scrape(
        self,
        date_from: date,
        date_to: date,
        save_html: Optional[str] = None,
    ) -> list[RawEvent]:
        all_events: list[RawEvent] = []
        seen_ids: set[str] = set()

        for venue_url in self._config.venues:
            slug = _venue_slug(venue_url)
            venue_events = self._scrape_venue(venue_url, slug, date_from, date_to, seen_ids)
            all_events.extend(venue_events)

        print(f"[scraper:bandsintown] Total: {len(all_events)} events across {len(self._config.venues)} venue(s)")
        return all_events

    def _scrape_venue(
        self,
        venue_url: str,
        slug: str,
        date_from: date,
        date_to: date,
        seen_ids: set[str],
    ) -> list[RawEvent]:
        events: list[RawEvent] = []
        param = _venue_param(venue_url)
        keywords = _venue_keywords(venue_url)
        url: Optional[str] = f"{ALL_DATES_API}?page=1&venue={param}"
        consecutive_empty = 0
        max_consecutive_empty = 5

        print(f"[scraper:bandsintown] Scraping venue: {slug} (matching: {keywords})")

        with httpx.Client(headers=self._headers, timeout=30, follow_redirects=True) as client:
            for page_num in range(1, self._config.max_pages + 1):
                if not url:
                    break

                for attempt in range(3):
                    try:
                        resp = client.get(url)
                        resp.raise_for_status()
                        break
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 416:
                            print(f"\n[scraper:bandsintown] Reached end of pages for {slug} at page {page_num}")
                            url = None
                        else:
                            raise
                        break
                    except (httpx.ReadTimeout, httpx.ConnectTimeout):
                        if attempt == 2:
                            raise
                        wait = 2 ** attempt
                        print(f"\n[scraper:bandsintown] Timeout on {slug} page {page_num}, retrying in {wait}s...")
                        time.sleep(wait)

                if url is None:
                    break

                data = resp.json()
                records = data.get("events", [])

                print(f"[scraper:bandsintown] {slug} page {page_num}: {len(records)} records", end="")

                page_events = 0
                past_window = False
                for record in records:
                    starts_at = record.get("startsAt", "")
                    if not starts_at:
                        continue

                    try:
                        event_dt = datetime.fromisoformat(starts_at)
                    except ValueError:
                        continue

                    event_date = event_dt.date()

                    if event_date > date_to:
                        past_window = True
                        break

                    if event_date < date_from:
                        continue

                    venue_name = record.get("venueName", "")
                    if not _matches_venue(venue_name, keywords):
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
                            venue_name=venue_name,
                            venue_city=record.get("locationText", ""),
                            event_datetime=event_dt.isoformat(timespec="seconds"),
                            ticket_url=event_url.split("?")[0] if event_url else None,
                            source=SOURCE,
                        )
                    )
                    page_events += 1

                print(f", {page_events} matched")

                if past_window:
                    print(f"[scraper:bandsintown] Passed {date_to} for {slug} — stopping")
                    break

                if page_events == 0:
                    consecutive_empty += 1
                    if consecutive_empty >= max_consecutive_empty:
                        print(f"[scraper:bandsintown] {max_consecutive_empty} consecutive empty pages for {slug} — stopping")
                        break
                else:
                    consecutive_empty = 0

                url = data.get("urlForNextPageOfEvents")
                if not url:
                    break

                time.sleep(1.0)

        print(f"[scraper:bandsintown] {slug}: {len(events)} events in date window")
        return events

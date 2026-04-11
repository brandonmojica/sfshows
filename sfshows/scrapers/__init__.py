from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class RawEvent:
    event_id: str
    artist_name: str
    venue_name: str
    venue_city: str
    event_datetime: str   # ISO8601
    ticket_url: Optional[str]
    source: str
    genre_label: Optional[str] = None  # filled in by enrichment, not the scraper


class BaseScraper(ABC):
    @abstractmethod
    async def scrape(self, date_from: date, date_to: date) -> list[RawEvent]:
        """Return all raw events in the given date window."""

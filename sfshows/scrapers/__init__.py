from abc import ABC, abstractmethod
from dataclasses import dataclass
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
    async def scrape(self, save_html: Optional[str] = None, limit: Optional[int] = None) -> list[RawEvent]:
        """Return all upcoming events from the source."""

from abc import ABC, abstractmethod
from typing import Optional


class BaseEnricher(ABC):
    @abstractmethod
    def enrich(self, artist_name: str) -> tuple[list[dict], Optional[str]]:
        """
        Return (tags_list, matched_genre_label_or_None).
        tags_list: [{"name": str, "count": int}]
        """

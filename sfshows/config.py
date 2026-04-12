from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import yaml


@dataclass(frozen=True)
class GenreRule:
    label: str
    tags: tuple[str, ...]  # MusicBrainz tag substrings to match


@dataclass(frozen=True)
class Config:
    # iMessage — one of these will be set, the other None
    recipients: tuple[str, ...]   # individual phone numbers / Apple IDs
    group_name: Optional[str]     # named group chat (mutually exclusive with recipients)

    # Schedule
    days_ahead: int

    # Genre rules (ordered — first match wins)
    genres: tuple[GenreRule, ...]

    # Active sources
    sources: tuple[str, ...]  # e.g. ("bandsintown", "billgraham")

    # Scraper
    venues: tuple[str, ...]       # Bandsintown venue URLs to scrape
    max_pages: int
    user_agent: str
    venue_concurrency: int        # Number of venues to scrape in parallel

    # Enrichment
    min_tag_count: int
    request_delay_ms: int
    cache_ttl_days: int

    # Database
    db_path: str  # already expanduser'd

    # Notification
    max_shows_per_digest: int
    include_ticket_url: bool


def load_config(path: str = "config.yaml") -> Config:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    imessage = raw["imessage"]
    recipients: tuple[str, ...] = ()
    group_name: Optional[str] = None
    if "recipients" in imessage:
        recipients = tuple(str(r) for r in imessage["recipients"])
    elif "group_name" in imessage:
        group_name = imessage["group_name"]
    else:
        raise ValueError("config.yaml imessage section must have 'recipients' or 'group_name'")

    genres = tuple(
        GenreRule(
            label=g["label"],
            tags=tuple(g["tags"]),
        )
        for g in raw["genres"]
    )

    sources = tuple(
        name for name, enabled in raw.get("sources", {}).items() if enabled
    )

    return Config(
        recipients=recipients,
        group_name=group_name,
        days_ahead=int(raw["schedule"]["days_ahead"]),
        genres=genres,
        sources=sources,
        venues=tuple(raw["scraper"]["venues"]),
        max_pages=int(raw["scraper"]["max_pages"]),
        user_agent=raw["scraper"]["user_agent"],
        venue_concurrency=int(raw["scraper"].get("venue_concurrency", 5)),
        min_tag_count=int(raw["enrichment"]["min_tag_count"]),
        request_delay_ms=int(raw["enrichment"]["request_delay_ms"]),
        cache_ttl_days=int(raw["enrichment"]["cache_ttl_days"]),
        db_path=os.path.expanduser(raw["database"]["path"]),
        max_shows_per_digest=int(raw["notification"]["max_shows_per_digest"]),
        include_ticket_url=bool(raw["notification"]["include_ticket_url"]),
    )

from __future__ import annotations

import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import httpx
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn

from sfshows.config import Config, GenreRule
from sfshows.console import console
from sfshows.db import Database
from sfshows.enrichment import BaseEnricher

BANDSINTOWN_API = "https://rest.bandsintown.com/artists/{name}?app_id=js_1.0"
MB_ARTIST_BY_ID = "https://musicbrainz.org/ws/2/artist/{mbid}?inc=tags+genres+url-rels+ratings&fmt=json"
MB_ARTIST_SEARCH = "https://musicbrainz.org/ws/2/artist/?query={name}&fmt=json"

MB_HEADERS = {
    "User-Agent": "sfshows/1.0 (github.com/brandonmojica/sfshows)",
    "Accept": "application/json",
}

# Parallel workers for MBID fetches (Bandsintown has no stated rate limit)
MBID_WORKERS = 8


def match_genre(
    tags: list[dict],
    genre_rules: tuple[GenreRule, ...],
    min_count: int,
) -> Optional[str]:
    """
    Check tags against genre rules in config order; return first match label.
    Tags below min_count are ignored.
    """
    filtered = [t for t in tags if t.get("count", 0) >= min_count]
    tag_names = [t["name"].lower() for t in filtered]

    for rule in genre_rules:
        for rule_tag in rule.tags:
            for tag_name in tag_names:
                if rule_tag.lower() in tag_name or tag_name in rule_tag.lower():
                    return rule.label
    return None


class MusicBrainzEnricher(BaseEnricher):
    def __init__(self, config: Config, db: Database) -> None:
        """Initialize the enricher with app config and a database handle for caching."""
        self._config = config
        self._db = db
        self._last_mb_call: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich(self, artist_name: str) -> tuple[list[dict], Optional[str]]:
        """Single-artist enrichment. Checks DB cache first."""
        cached = self._db.get_cached_genre(artist_name, self._config.cache_ttl_days)
        if cached is not None:
            return cached.tags, cached.genre_label
        return self._fetch_and_cache(artist_name)

    def enrich_batch(
        self, artist_names: list[str]
    ) -> dict[str, Optional[str]]:
        """
        Enrich a list of unique artist names and return {artist_name: genre_label}.

        Steps:
        1. Batch-check DB cache — zero API calls for already-cached artists.
        2. Parallel-fetch MBIDs from Bandsintown for cache misses.
        3. Sequential MusicBrainz tag lookups (rate-limited to 1/sec).
        4. Store results in DB cache.
        """
        if not artist_names:
            return {}

        results: dict[str, Optional[str]] = {}
        misses: list[str] = []

        # Step 1: batch cache lookup
        for name in artist_names:
            cached = self._db.get_cached_genre(name, self._config.cache_ttl_days)
            if cached is not None:
                results[name] = cached.genre_label
            else:
                misses.append(name)

        cache_hits = len(artist_names) - len(misses)
        if misses:
            console.print(
                f"  [dim]{cache_hits} cached,[/] [bold]{len(misses)}[/] [dim]need lookup[/]"
            )
        else:
            console.print(f"  [dim]All {cache_hits} artists cached — skipping API calls[/]")
            return results

        # Step 2: parallel MBID fetches for cache misses
        mbid_map: dict[str, Optional[str]] = {}  # artist_name -> mbid or None
        with ThreadPoolExecutor(max_workers=MBID_WORKERS) as pool:
            future_to_name = {
                pool.submit(self._fetch_mbid, name): name for name in misses
            }
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    mbid_map[name] = future.result()
                except Exception:
                    mbid_map[name] = None

        # Step 3: sequential MusicBrainz lookups (1/sec rate limit)
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Enriching artists", total=len(misses))
            for name in misses:
                mbid = mbid_map.get(name)
                data = self._fetch_artist_data(mbid) if mbid else self._fetch_artist_data_by_name(name)
                tags = data.get("tags", [])
                genre_label = match_genre(tags, self._config.genres, self._config.min_tag_count)
                self._db.set_cached_genre(
                    name, mbid, tags, genre_label,
                    artist_type=data.get("artist_type"),
                    country=data.get("country"),
                    area=data.get("area"),
                    begin_date=data.get("begin_date"),
                    end_date=data.get("end_date"),
                    ended=data.get("ended"),
                    mb_genres=data.get("mb_genres"),
                    rating=data.get("rating"),
                    rating_votes=data.get("rating_votes"),
                    urls=data.get("urls"),
                )
                results[name] = genre_label
                progress.advance(task_id)

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_and_cache(self, artist_name: str) -> tuple[list[dict], Optional[str]]:
        """Fetch full MusicBrainz data for one artist, cache it, and return (tags, genre_label)."""
        mbid = self._fetch_mbid(artist_name)
        data = self._fetch_artist_data(mbid) if mbid else self._fetch_artist_data_by_name(artist_name)
        tags = data.get("tags", [])
        genre_label = match_genre(tags, self._config.genres, self._config.min_tag_count)
        self._db.set_cached_genre(
            artist_name, mbid, tags, genre_label,
            artist_type=data.get("artist_type"),
            country=data.get("country"),
            area=data.get("area"),
            begin_date=data.get("begin_date"),
            end_date=data.get("end_date"),
            ended=data.get("ended"),
            mb_genres=data.get("mb_genres"),
            rating=data.get("rating"),
            rating_votes=data.get("rating_votes"),
            urls=data.get("urls"),
        )
        return tags, genre_label

    def _fetch_mbid(self, artist_name: str) -> Optional[str]:
        """Call Bandsintown REST API to get the MusicBrainz ID for an artist."""
        url = BANDSINTOWN_API.format(name=urllib.parse.quote(artist_name))
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                mbid = data.get("mbid", "").strip()
                return mbid if mbid else None
        except Exception:
            pass
        return None

    def _fetch_artist_data(self, mbid: str) -> dict:
        """Query MusicBrainz by MBID and return parsed artist data dict."""
        self._rate_limit()
        url = MB_ARTIST_BY_ID.format(mbid=urllib.parse.quote(mbid))
        try:
            with httpx.Client(timeout=10, headers=MB_HEADERS) as client:
                resp = client.get(url)
            if resp.status_code == 200:
                return self._parse_artist_response(resp.json())
        except Exception:
            pass
        return {}

    def _fetch_artist_data_by_name(self, artist_name: str) -> dict:
        """Fallback: search MusicBrainz by name, use top-score result."""
        self._rate_limit()
        url = MB_ARTIST_SEARCH.format(name=urllib.parse.quote(artist_name))
        try:
            with httpx.Client(timeout=10, headers=MB_HEADERS) as client:
                resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                artists = data.get("artists", [])
                if artists:
                    top = max(artists, key=lambda a: a.get("score", 0))
                    mbid = top.get("id")
                    if mbid:
                        return self._fetch_artist_data(mbid)
        except Exception:
            pass
        return {}

    @staticmethod
    def _parse_artist_response(data: dict) -> dict:
        """Extract all enriched fields from a MusicBrainz artist JSON response."""
        tags = [
            {"name": t["name"], "count": t["count"]}
            for t in data.get("tags", [])
        ]
        mb_genres = [
            {"name": g["name"], "count": g["count"]}
            for g in data.get("genres", [])
        ]
        urls = [
            {"type": r.get("type", ""), "url": r.get("url", {}).get("resource", "")}
            for r in data.get("relations", [])
            if r.get("url")
        ]
        life_span = data.get("life-span", {})
        rating_data = data.get("rating", {})
        return {
            "tags": tags,
            "mb_genres": mb_genres,
            "urls": urls,
            "artist_type": data.get("type"),
            "country": data.get("country"),
            "area": (data.get("area") or {}).get("name"),
            "begin_date": life_span.get("begin"),
            "end_date": life_span.get("end"),
            "ended": life_span.get("ended"),
            "rating": rating_data.get("value"),
            "rating_votes": rating_data.get("votes-count"),
        }

    def _rate_limit(self) -> None:
        """Enforce minimum delay between MusicBrainz API calls."""
        delay_s = self._config.request_delay_ms / 1000.0
        elapsed = time.monotonic() - self._last_mb_call
        if elapsed < delay_s:
            time.sleep(delay_s - elapsed)
        self._last_mb_call = time.monotonic()

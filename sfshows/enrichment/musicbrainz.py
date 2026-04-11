from __future__ import annotations

import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import httpx

from sfshows.config import Config, GenreRule
from sfshows.db import Database
from sfshows.enrichment import BaseEnricher

BANDSINTOWN_API = "https://rest.bandsintown.com/artists/{name}?app_id=js_1.0"
MB_ARTIST_BY_ID = "https://musicbrainz.org/ws/2/artist/{mbid}?inc=tags&fmt=json"
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
            print(
                f"[enricher] {cache_hits} cached, {len(misses)} need lookup"
            )
        else:
            print(f"[enricher] All {cache_hits} artists cached — skipping API calls")
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

        # Step 3: sequential MusicBrainz tag lookups (1/sec rate limit)
        for i, name in enumerate(misses, 1):
            mbid = mbid_map.get(name)
            if mbid:
                tags = self._fetch_tags_by_mbid(mbid)
            else:
                tags = self._fetch_tags_by_name(name)

            genre_label = match_genre(tags, self._config.genres, self._config.min_tag_count)
            self._db.set_cached_genre(name, mbid, tags, genre_label)
            results[name] = genre_label

            if i % 10 == 0 or i == len(misses):
                print(f"[enricher] {i}/{len(misses)} looked up")

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_and_cache(self, artist_name: str) -> tuple[list[dict], Optional[str]]:
        mbid = self._fetch_mbid(artist_name)
        tags = self._fetch_tags_by_mbid(mbid) if mbid else self._fetch_tags_by_name(artist_name)
        genre_label = match_genre(tags, self._config.genres, self._config.min_tag_count)
        self._db.set_cached_genre(artist_name, mbid, tags, genre_label)
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

    def _fetch_tags_by_mbid(self, mbid: str) -> list[dict]:
        """Query MusicBrainz by MBID and return tag list."""
        self._rate_limit()
        url = MB_ARTIST_BY_ID.format(mbid=urllib.parse.quote(mbid))
        try:
            with httpx.Client(timeout=10, headers=MB_HEADERS) as client:
                resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return [
                    {"name": t["name"], "count": t["count"]}
                    for t in data.get("tags", [])
                ]
        except Exception:
            pass
        return []

    def _fetch_tags_by_name(self, artist_name: str) -> list[dict]:
        """Fallback: search MusicBrainz by name and use the top-score result's tags."""
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
                        return self._fetch_tags_by_mbid(mbid)
        except Exception:
            pass
        return []

    def _rate_limit(self) -> None:
        """Enforce minimum delay between MusicBrainz API calls."""
        delay_s = self._config.request_delay_ms / 1000.0
        elapsed = time.monotonic() - self._last_mb_call
        if elapsed < delay_s:
            time.sleep(delay_s - elapsed)
        self._last_mb_call = time.monotonic()

#!/usr/bin/env python3
"""
Standalone script to backfill / refresh extended MusicBrainz data for all
artists already in the database.

Usage (run from project root):
    python scripts/enrich_artists.py                  # skip already-enriched artists
    python scripts/enrich_artists.py --force          # re-fetch everyone
    python scripts/enrich_artists.py --artist "Radiohead"  # single artist
    python scripts/enrich_artists.py --missing-only   # only rows with no mbid yet

The script reads config.yaml for db_path and rate-limit settings.
"""
from __future__ import annotations

import argparse
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import httpx
from tqdm import tqdm

from sfshows.config import load_config
from sfshows.db import Database
from sfshows.enrichment.musicbrainz import (
    BANDSINTOWN_API,
    MB_HEADERS,
    MusicBrainzEnricher,
    match_genre,
)

# ── helpers ──────────────────────────────────────────────────────────────────

def fetch_mbid(artist_name: str) -> Optional[str]:
    url = BANDSINTOWN_API.format(name=urllib.parse.quote(artist_name))
    try:
        resp = httpx.get(url, timeout=10)
        if resp.status_code == 200:
            mbid = resp.json().get("mbid", "").strip()
            return mbid or None
    except Exception:
        pass
    return None


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill MusicBrainz artist data")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch all artists, even those already enriched",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        dest="missing_only",
        help="Only fetch artists with no mbid recorded",
    )
    parser.add_argument(
        "--artist",
        metavar="NAME",
        help="Enrich a single artist by name",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="PATH",
        help="Path to config.yaml (default: config.yaml)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    db = Database(config.db_path)
    db.init_schema()

    enricher = MusicBrainzEnricher(config, db)
    delay_s = config.request_delay_ms / 1000.0

    # ── determine which artists to process ───────────────────────────────────
    if args.artist:
        if not db.artist_in_db(args.artist):
            print(f"Artist '{args.artist}' not found in database.")
            return
        artists = [args.artist]

    elif args.missing_only:
        artists = db.get_artists_to_enrich(missing_only=True)
        print(f"Found {len(artists)} artists with no MBID.")

    elif args.force:
        artists = db.get_artists_to_enrich(force=True)
        print(f"Force mode: processing all {len(artists)} artists from shows table.")

    else:
        artists = db.get_artists_to_enrich()
        print(f"Found {len(artists)} artists needing extended enrichment.")

    if not artists:
        print("Nothing to do.")
        return

    # ── fetch MBIDs in parallel for any artist that needs one ────────────────
    mbid_map: dict[str, Optional[str]] = {}
    needs_mbid = []
    for name in artists:
        cached_mbid = db.get_artist_mbid(name)
        if cached_mbid:
            mbid_map[name] = cached_mbid
        else:
            needs_mbid.append(name)

    if needs_mbid:
        print(f"Fetching MBIDs for {len(needs_mbid)} artists via Bandsintown…")
        with ThreadPoolExecutor(max_workers=8) as pool:
            future_to_name = {pool.submit(fetch_mbid, n): n for n in needs_mbid}
            for future in tqdm(
                as_completed(future_to_name), total=len(needs_mbid), unit="artist"
            ):
                name = future_to_name[future]
                try:
                    mbid_map[name] = future.result()
                except Exception:
                    mbid_map[name] = None

    # ── sequential MusicBrainz lookups (rate-limited) ─────────────────────────
    print(f"Fetching extended MusicBrainz data for {len(artists)} artists…")
    last_call = 0.0

    for name in tqdm(artists, desc="MusicBrainz lookup", unit="artist"):
        mbid = mbid_map.get(name)

        # rate limit
        elapsed = time.monotonic() - last_call
        if elapsed < delay_s:
            time.sleep(delay_s - elapsed)
        last_call = time.monotonic()

        data = (
            enricher._fetch_artist_data(mbid)
            if mbid
            else enricher._fetch_artist_data_by_name(name)
        )

        tags = data.get("tags", [])
        genre_label = match_genre(tags, config.genres, config.min_tag_count)

        db.set_cached_genre(
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

    db.close()
    print("Done.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
sfshows — SF concert scraper and iMessage notifier.

Usage:
    python3 run.py              # Full run: scrape + enrich + notify
    python3 run.py --dry-run    # Scrape and print digest; don't send iMessage
    python3 run.py --notify-only # Skip scraping; send pending shows already in DB
    python3 run.py --scrape-only # Scrape and store; don't notify
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sys
import traceback
from datetime import datetime, timedelta

from sfshows.config import load_config
from sfshows.db import Database, ShowRecord
from sfshows.digest import format_digest
from sfshows.enrichment.musicbrainz import MusicBrainzEnricher
from sfshows.notifier import NotificationError, send_imessage
from sfshows.scrapers import BaseScraper, RawEvent
from sfshows.scrapers.bandsintown import BandsintownScraper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SF shows scraper and iMessage notifier")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape and print digest to stdout; do not send iMessage",
    )
    group.add_argument(
        "--notify-only",
        action="store_true",
        help="Skip scraping; send whatever pending shows are already in the DB",
    )
    group.add_argument(
        "--scrape-only",
        action="store_true",
        help="Scrape and store shows; do not send any notification",
    )
    parser.add_argument(
        "--save-html",
        metavar="FILE",
        help="Save the raw rendered HTML from the scraper to FILE (for debugging selectors)",
    )
    parser.add_argument(
        "--csv",
        metavar="FILE",
        help="Export scraped shows to a CSV file (e.g. shows.csv)",
    )
    return parser.parse_args()


def export_csv(shows: list[ShowRecord], path: str) -> None:
    fields = ["artist_name", "venue_name", "venue_city", "event_datetime", "genre_label", "ticket_url", "source", "event_id"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for show in shows:
            writer.writerow({
                "artist_name":   show.artist_name,
                "venue_name":    show.venue_name,
                "venue_city":    show.venue_city,
                "event_datetime": show.event_datetime,
                "genre_label":   show.genre_label or "",
                "ticket_url":    show.ticket_url or "",
                "source":        show.source,
                "event_id":      show.event_id,
            })
    print(f"[csv] Wrote {len(shows)} shows to {path}")


async def run_scrape(cfg, db, save_html: str = None) -> tuple[int, int, list]:
    """Scrape, enrich genres, and upsert shows. Returns (scraped_count, new_count, scraped_shows)."""
    scraper_registry: dict[str, BaseScraper] = {
        "bandsintown": BandsintownScraper(cfg),
    }

    raw_events: list[RawEvent] = []
    for source in cfg.sources:
        scraper = scraper_registry.get(source)
        if scraper is None:
            print(f"[run] Unknown source '{source}' in config — skipping")
            continue
        raw_events.extend(await scraper.scrape(save_html=save_html))

    enricher = MusicBrainzEnricher(cfg, db)

    # Deduplicate artists — look each up once regardless of how many shows they have
    unique_artists = list({e.artist_name for e in raw_events})
    print(f"[run] {len(raw_events)} events, {len(unique_artists)} unique artists")
    genre_map = enricher.enrich_batch(unique_artists)

    new_count = 0
    scraped_shows = []
    for event in raw_events:
        genre_label = genre_map.get(event.artist_name)

        show = ShowRecord(
            event_id=event.event_id,
            source=event.source,
            artist_name=event.artist_name,
            venue_name=event.venue_name,
            venue_city=event.venue_city,
            event_datetime=event.event_datetime,
            ticket_url=event.ticket_url,
            genre_label=genre_label,
        )
        scraped_shows.append(show)
        is_new = db.upsert_show(show)
        if is_new:
            new_count += 1
            print(f"[new] {show.artist_name} @ {show.venue_name} — {show.event_datetime} [{genre_label}]")

    return len(raw_events), new_count, scraped_shows


async def main() -> None:
    args = parse_args()
    cfg = load_config("config.yaml")

    db = Database(cfg.db_path)
    db.init_schema()

    scraped_count = 0
    new_count = 0
    notified_count = 0
    error_msg = None

    try:
        # Step 1: Scrape (unless --notify-only)
        scraped_shows = []
        if not args.notify_only:
            scraped_count, new_count, scraped_shows = await run_scrape(cfg, db, save_html=args.save_html)
            print(f"[run] Scraped {scraped_count} events, {new_count} new")

        # Step 1b: CSV export (shows from this scrape only)
        if args.csv:
            export_csv(scraped_shows, args.csv)

        # Step 2: Notify (unless --scrape-only)
        if not args.scrape_only:
            pending = db.get_pending_shows()
            if not pending:
                print("[run] No pending shows to notify about")
                db.log_run(scraped_count, new_count, 0, None)
                return

            date_from_dt = datetime.now()
            date_to_dt = date_from_dt + timedelta(days=cfg.days_ahead)

            capped = pending[: cfg.max_shows_per_digest]
            message = format_digest(
                shows=capped,
                include_ticket_url=cfg.include_ticket_url,
                date_from=date_from_dt,
                date_to=date_to_dt,
                total_pending=len(pending),
            )

            if args.dry_run:
                print("\n" + "=" * 60)
                print("DRY RUN — digest preview (not sent):")
                print("=" * 60)
                print(message)
                print("=" * 60)
            else:
                targets = list(cfg.recipients) + ([cfg.group_name] if cfg.group_name else [])
                print(f"[run] Sending digest ({len(capped)} shows) to: {', '.join(targets)}")
                send_imessage(message, recipients=cfg.recipients, group_name=cfg.group_name)
                db.mark_notified([s.event_id for s in pending])
                notified_count = len(pending)
                print(f"[run] Sent and marked {notified_count} shows as notified")

    except NotificationError as e:
        error_msg = str(e)
        print(f"[error] iMessage failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"[error] {e}", file=sys.stderr)
        traceback.print_exc()
        db.log_run(scraped_count, new_count, notified_count, error_msg)
        sys.exit(1)
    finally:
        db.log_run(scraped_count, new_count, notified_count, error_msg)
        db.close()


if __name__ == "__main__":
    asyncio.run(main())

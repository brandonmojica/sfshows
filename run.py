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
import dataclasses
import random
import sys
from datetime import datetime, timedelta

from rich.panel import Panel
from rich.table import Table

from sfshows.config import load_config
from sfshows.console import console
from sfshows.db import Database, ShowRecord
from sfshows.digest import format_digest
from sfshows.enrichment.musicbrainz import MusicBrainzEnricher
from sfshows.notifier import NotificationError, send_imessage
from sfshows.scrapers import BaseScraper, RawEvent
from sfshows.scrapers.bandsintown import BandsintownScraper


def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
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
        "--limit",
        metavar="N",
        type=int,
        help="Only scrape the first N venues (useful for quick debugging)",
    )
    parser.add_argument(
        "--csv",
        metavar="FILE",
        help="Export all shows from the DB to a CSV file. Skips scraping unless combined with another mode flag.",
    )
    return parser.parse_args()


def export_csv(shows: list[dict], path: str) -> None:
    """Write all shows (joined with enrichment data) to a CSV file at the given path."""
    fields = [
        "artist_name", "venue_name", "venue_city", "event_datetime",
        "genre_label", "ticket_url", "source", "event_id",
        "notified", "created_at",
        "mbid", "artist_type", "country", "area",
        "begin_date", "end_date", "ended",
        "rating", "rating_votes",
        "tags_json", "mb_genres_json", "urls_json",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shows)
    console.print(f"  [dim]Exported[/] [bold]{len(shows)}[/] [dim]shows to[/] [cyan]{path}[/]")


async def run_scrape(cfg, db, save_html: str = None, limit: int = None) -> tuple[int, int, list, list]:
    """Scrape, enrich genres, and upsert shows. Returns (scraped_count, new_count, scraped_shows, new_shows)."""
    scraper_registry: dict[str, BaseScraper] = {
        "bandsintown": BandsintownScraper(cfg),
    }

    raw_events: list[RawEvent] = []
    for source in cfg.sources:
        scraper = scraper_registry.get(source)
        if scraper is None:
            console.print(f"  [yellow]Unknown source '{source}' in config — skipping[/]")
            continue
        raw_events.extend(await scraper.scrape(save_html=save_html, limit=limit))

    enricher = MusicBrainzEnricher(cfg, db)

    # Deduplicate artists — look each up once regardless of how many shows they have
    unique_artists = list({e.artist_name for e in raw_events})
    console.print(
        f"  [dim]{len(raw_events)} events,[/] [bold]{len(unique_artists)}[/] [dim]unique artists[/]"
    )
    genre_map = enricher.enrich_batch(unique_artists)

    new_count = 0
    scraped_shows = []
    new_shows = []
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
            new_shows.append(show)
            console.print(
                f"  [cyan]•[/] [bold]{show.artist_name}[/] [dim]@[/] {show.venue_name}"
                f"  [dim]{show.event_datetime}[/]"
                + (f"  [yellow]{genre_label}[/]" if genre_label else "")
            )

    return len(raw_events), new_count, scraped_shows, new_shows


def _print_summary(
    scraped: int, new: int, notified: int, new_shows: list[ShowRecord], elapsed: float
) -> None:
    """Print a Rich panel summarizing the run stats and any new shows found."""
    minutes, seconds = divmod(int(elapsed), 60)
    elapsed_str = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"

    stats = Table.grid(padding=(0, 3))
    stats.add_column(style="dim")
    stats.add_column(style="bold")
    stats.add_row("Scraped", str(scraped))
    stats.add_row("New", str(new))
    stats.add_row("Notified", str(notified))
    stats.add_row("Time", elapsed_str)
    console.print(Panel(stats, title="[bold]Run Complete[/]", border_style="green", padding=(1, 3)))

    if new_shows:
        table = Table(show_header=True, header_style="bold dim", box=None, padding=(0, 2))
        table.add_column("Artist", style="bold")
        table.add_column("Venue", style="")
        table.add_column("Date", style="dim")
        table.add_column("Genre", style="yellow")
        for show in new_shows:
            dt = show.event_datetime
            date_str = dt.strftime("%-d %b") if hasattr(dt, "strftime") else str(dt)
            table.add_row(
                show.artist_name,
                show.venue_name,
                date_str,
                show.genre_label or "—",
            )
        console.print(table)


async def main() -> None:
    """Entry point: parse args, then run the scrape and/or notification pipeline."""
    import time
    start_time = time.monotonic()

    args = parse_args()
    cfg = load_config("config.yaml")

    db = Database(cfg.db_path)
    db.init_schema()

    pruned = db.delete_past_shows()
    if pruned:
        console.print(f"  [dim]Pruned {pruned} past show(s) from database.[/dim]")

    scraped_count = 0
    new_count = 0
    notified_count = 0
    error_msg = None
    new_shows: list[ShowRecord] = []

    # When --csv is the only flag, skip scraping and notification — just dump the DB.
    csv_only = bool(args.csv) and not (args.notify_only or args.dry_run or args.scrape_only)

    try:
        # Step 1: Scrape (unless --notify-only or csv_only)
        scraped_shows = []
        if not args.notify_only and not csv_only:
            scraped_count, new_count, scraped_shows, new_shows = await run_scrape(
                cfg, db, save_html=args.save_html, limit=args.limit
            )
            console.print(
                f"  [dim]Scraped[/] [bold]{scraped_count}[/] [dim]events,[/] "
                f"[bold green]{new_count}[/] [dim]new[/]"
            )

        # Step 1b: CSV export (all shows in DB, joined with enrichment data)
        if args.csv:
            export_csv(db.get_all_shows(), args.csv)

        # Step 1c: Google Sheets sync
        if cfg.sheets_credentials_path and cfg.sheets_spreadsheet_id and not csv_only:
            from sfshows.sheets import sync_shows_to_sheet
            all_shows = db.get_all_shows()
            sheets_url = sync_shows_to_sheet(all_shows, cfg)
            cfg = dataclasses.replace(cfg, all_shows_url=sheets_url)
            console.print(f"  [dim]Sheet synced:[/] [cyan]{sheets_url}[/]")

        # Step 2: Notify (unless --scrape-only or csv_only)
        if not args.scrape_only and not csv_only:
            pending = db.get_pending_shows()
            if not pending:
                console.print("  [dim]No pending shows to notify about[/]")
                db.log_run(scraped_count, new_count, notified_count, error_msg)
                db.close()
                _print_summary(scraped_count, new_count, notified_count, new_shows, time.monotonic() - start_time)
                return

            date_from_dt = datetime.now()
            date_to_dt = date_from_dt + timedelta(days=cfg.days_ahead)

            # pending is already sorted by created_at DESC (newest scrape first)
            if len(pending) > cfg.max_shows_per_digest:
                capped = random.sample(pending, cfg.max_shows_per_digest)
                capped.sort(key=lambda s: s.event_datetime)
            else:
                capped = sorted(pending, key=lambda s: s.artist_name.lower())
            message = format_digest(
                shows=capped,
                include_ticket_url=cfg.include_ticket_url,
                date_from=date_from_dt,
                date_to=date_to_dt,
                total_pending=len(pending),
                all_shows_url=cfg.all_shows_url,
            )

            if args.dry_run:
                console.print(
                    Panel(
                        message,
                        title="[bold yellow]DRY RUN — digest preview (not sent)[/]",
                        border_style="yellow",
                        padding=(1, 2),
                    )
                )
            else:
                targets = list(cfg.recipients) + ([cfg.group_name] if cfg.group_name else [])
                console.print(
                    f"  [dim]Sending digest ({len(capped)} shows) to:[/] "
                    f"[bold]{', '.join(targets)}[/]"
                )
                send_imessage(message, recipients=cfg.recipients, group_name=cfg.group_name)
                db.mark_notified([s.event_id for s in pending])
                notified_count = len(pending)

    except NotificationError as e:
        error_msg = str(e)
        console.print(
            Panel(str(e), title="[bold red]iMessage Error[/]", border_style="red"),
        )
        db.log_run(scraped_count, new_count, notified_count, error_msg)
        db.close()
        sys.exit(1)
    except Exception:
        console.print_exception()
        error_msg = "exception (see above)"
        db.log_run(scraped_count, new_count, notified_count, error_msg)
        db.close()
        sys.exit(1)

    db.log_run(scraped_count, new_count, notified_count, error_msg)
    db.close()

    _print_summary(scraped_count, new_count, notified_count, new_shows, time.monotonic() - start_time)


if __name__ == "__main__":
    asyncio.run(main())

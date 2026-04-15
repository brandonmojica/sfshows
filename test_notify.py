#!/usr/bin/env python3
"""
Send a test iMessage digest using 5 randomly sampled shows from the last scraped day.
Shows are NOT marked as notified — safe to rerun.
"""
import random
from datetime import datetime

from sfshows.config import load_config
from sfshows.db import Database
from sfshows.digest import format_digest
from sfshows.notifier import send_imessage


def main():
    cfg = load_config()
    db = Database(cfg.db_path)
    db.init_schema()

    shows = db.get_shows_from_latest_scrape_date()
    if not shows:
        print("No shows found in the database.")
        return

    scrape_date = shows[0].created_at[:10]
    print(f"Last scrape date: {scrape_date}  ({len(shows)} shows available)")

    sample = random.sample(shows, min(5, len(shows)))

    # Date range for digest header — use full pool, not just the sample
    datetimes = []
    for s in shows:
        try:
            datetimes.append(datetime.fromisoformat(s.event_datetime))
        except ValueError:
            pass
    date_from = min(datetimes).date() if datetimes else None
    date_to = max(datetimes).date() if datetimes else None

    digest = format_digest(
        shows=sample,
        include_ticket_url=cfg.include_ticket_url,
        date_from=date_from,
        date_to=date_to,
        total_pending=len(shows),
        all_shows_url=cfg.all_shows_url,
    )

    print("\n--- Digest Preview ---")
    print(digest)
    print("----------------------\n")

    recipients = cfg.recipients
    group_name = cfg.group_name
    send_imessage(digest, recipients=recipients, group_name=group_name)
    print("Test message sent.")


if __name__ == "__main__":
    main()

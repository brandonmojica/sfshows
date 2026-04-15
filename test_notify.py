#!/usr/bin/env python3
"""
Send a test iMessage digest matching the real run.py format exactly.
Shows are NOT marked as notified — safe to rerun.
"""
import random
from datetime import datetime, timedelta

from sfshows.config import load_config
from sfshows.db import Database
from sfshows.digest import format_digest
from sfshows.notifier import send_imessage
from sfshows.sheets import sync_shows_to_sheet


def main():
    cfg = load_config()
    db = Database(cfg.db_path)
    db.init_schema()

    # Use same pool as real run: all unnotified shows
    pending = db.get_pending_shows()
    if not pending:
        print("No pending shows found in the database.")
        return

    print(f"Pending shows: {len(pending)}")

    # Same sampling/sorting logic as run.py
    if len(pending) > cfg.max_shows_per_digest:
        sample = random.sample(pending, cfg.max_shows_per_digest)
        sample.sort(key=lambda s: s.event_datetime)
    else:
        sample = sorted(pending, key=lambda s: s.artist_name.lower())

    # Date range from actual min/max of unnotified shows
    datetimes = []
    for s in pending:
        try:
            datetimes.append(datetime.fromisoformat(s.event_datetime))
        except ValueError:
            pass
    date_from = min(datetimes) if datetimes else datetime.now()
    date_to = max(datetimes) if datetimes else date_from

    all_shows_url = cfg.all_shows_url
    if cfg.sheets_credentials_path and cfg.sheets_spreadsheet_id:
        all_shows = db.get_all_shows()
        all_shows_url = sync_shows_to_sheet(all_shows, cfg)

    digest = format_digest(
        shows=sample,
        include_ticket_url=cfg.include_ticket_url,
        date_from=date_from,
        date_to=date_to,
        total_pending=len(pending),
        all_shows_url=all_shows_url,
    )

    print("\n--- Digest Preview ---")
    print(digest)
    print("----------------------\n")

    send_imessage(digest, recipients=cfg.recipients, group_name=cfg.group_name)
    print("Test message sent.")


if __name__ == "__main__":
    main()

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Optional

from sfshows.db import ShowRecord


def format_digest(
    shows: list[ShowRecord],
    include_ticket_url: bool,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    total_pending: int = 0,
) -> str:
    """
    Build a single digest string suitable for iMessage.

    Groups shows by date, sorted ascending. Example:

        SF Shows — Sat Apr 12 – Fri Apr 25
        14 new shows

        SAT APR 12
        • The Strokes @ The Fillmore, 8:00 PM  [Indie/Alternative]
          tickets: https://...

        SUN APR 13
        • ODESZA @ Chase Center, 9:00 PM  [Electronic/Dance]
    """
    if not shows:
        return "No new SF shows found for the upcoming period."

    lines: list[str] = []

    # Header
    if date_from and date_to:
        from_str = date_from.strftime("%a %b %-d")
        to_str = date_to.strftime("%a %b %-d")
        lines.append(f"SF Shows — {from_str} – {to_str}")
    else:
        lines.append("SF Shows — Upcoming")

    count_line = f"{len(shows)} new show{'s' if len(shows) != 1 else ''}"
    if total_pending > len(shows):
        count_line += f" (showing {len(shows)} of {total_pending})"
    lines.append(count_line)

    # Group by date
    by_date: dict[str, list[ShowRecord]] = defaultdict(list)
    for show in shows:
        try:
            dt = datetime.fromisoformat(show.event_datetime)
            date_key = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_key = "Unknown Date"
        by_date[date_key].append(show)

    for date_key in sorted(by_date.keys()):
        lines.append("")
        try:
            dt = datetime.strptime(date_key, "%Y-%m-%d")
            lines.append(dt.strftime("%a %b %-d").upper())
        except ValueError:
            lines.append(date_key.upper())

        for show in by_date[date_key]:
            try:
                dt = datetime.fromisoformat(show.event_datetime)
                time_str = dt.strftime("%-I:%M %p")
            except ValueError:
                time_str = ""

            genre_str = f"  [{show.genre_label}]" if show.genre_label else ""
            time_part = f", {time_str}" if time_str else ""
            show_line = f"• {show.artist_name} @ {show.venue_name}{time_part}{genre_str}"
            lines.append(show_line)

            if include_ticket_url and show.ticket_url:
                lines.append(f"  tickets: {show.ticket_url}")

    if total_pending > len(shows):
        remaining = total_pending - len(shows)
        lines.append("")
        lines.append(f"... and {remaining} more show{'s' if remaining != 1 else ''}")

    return "\n".join(lines)

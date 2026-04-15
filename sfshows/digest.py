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
    all_shows_url: str = "",
) -> str:
    """
    Build a single digest string suitable for iMessage.

    Groups shows by venue, sorted alphabetically. Example:

        SF Shows — Apr 14–28
        5 new shows (showing 5 of 23)

        BOTTOM OF THE HILL
        • Yard Act — Wed Apr 15, 8pm

        THE FILLMORE
        • The Strokes — Sat Apr 19, 8pm
    """
    if not shows:
        return "No new SF shows found for the upcoming period."

    lines: list[str] = []

    # Header box
    if date_from and date_to:
        from_str = date_from.strftime("%b %-d")
        if date_to.month == date_from.month:
            to_str = date_to.strftime("%-d")
        else:
            to_str = date_to.strftime("%b %-d")
        title = f"SF Shows — {from_str}–{to_str}"
    else:
        title = "SF Shows — Upcoming"

    lines.append(f"🪩 {title} 🪩")

    # Group by venue
    by_venue: dict[str, list[ShowRecord]] = defaultdict(list)
    for show in shows:
        by_venue[show.venue_name].append(show)

    for venue in sorted(by_venue.keys()):
        lines.append("")
        lines.append(venue.upper())

        # Sort shows within each venue by date
        venue_shows = sorted(by_venue[venue], key=lambda s: s.event_datetime)
        for show in venue_shows:
            try:
                dt = datetime.fromisoformat(show.event_datetime)
                date_str = dt.strftime("%a %b %-d")
                if dt.hour == 0 and dt.minute == 0:
                    show_line = f"• {show.artist_name} — {date_str}"
                else:
                    time_raw = dt.strftime("%-I:%M %p")
                    time_str = time_raw.replace(":00", "").replace(" AM", "am").replace(" PM", "pm")
                    show_line = f"• {show.artist_name} — {date_str} @ {time_str}"
            except ValueError:
                show_line = f"• {show.artist_name}"
            lines.append(show_line)

            if include_ticket_url and show.ticket_url:
                lines.append(f"  tickets: {show.ticket_url}")

    if total_pending > len(shows):
        remaining = total_pending - len(shows)
        lines.append("")
        more_line = f"... and {remaining} more show{'s' if remaining != 1 else ''}"
        if all_shows_url:
            more_line += f" → {all_shows_url} (tap to see all)"
        lines.append(more_line)
    elif all_shows_url:
        lines.append("")
        lines.append(f"→ {all_shows_url} (tap to see all)")

    return "\n".join(lines)

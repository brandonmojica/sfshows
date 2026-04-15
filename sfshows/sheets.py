from __future__ import annotations

import urllib.parse
import urllib.request
from datetime import datetime
from typing import TYPE_CHECKING

import gspread

if TYPE_CHECKING:
    from sfshows.config import Config


def shorten_url(url: str) -> str:
    """Shorten a URL using TinyURL. Returns original on failure."""
    try:
        api = f"https://tinyurl.com/api-create.php?url={urllib.parse.quote(url, safe='')}"
        with urllib.request.urlopen(api, timeout=5) as resp:
            return resp.read().decode().strip()
    except Exception:
        return url


def _build_rows(shows: list[dict]) -> list[list]:
    header = ["Artist", "Venue", "City", "Date", "Time", "Genre", "Ticket URL", "Notified"]
    rows = [header]
    for show in shows:
        dt_raw = show.get("event_datetime", "")
        date_str = ""
        time_str = ""
        if dt_raw:
            try:
                dt = datetime.fromisoformat(dt_raw)
                date_str = dt.strftime("%a %b %-d, %Y")
                if dt.hour or dt.minute:
                    time_str = dt.strftime("%-I:%M %p")
            except ValueError:
                date_str = dt_raw

        rows.append([
            show.get("artist_name", ""),
            show.get("venue_name", ""),
            show.get("venue_city", ""),
            date_str,
            time_str,
            show.get("genre_label") or "",
            show.get("ticket_url") or "",
            "Yes" if show.get("notified") else "No",
        ])
    return rows


def _get_or_create_worksheet(spreadsheet, title: str, index: int):
    """Return the worksheet with the given title, creating it if needed."""
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=2000, cols=10, index=index)


def sync_shows_to_sheet(shows: list[dict], cfg: "Config") -> str:
    """
    Upload shows to two worksheets:
      - 'All Shows'       — every show in the DB
      - 'Recently Found'  — unnotified shows only

    Returns the shortened spreadsheet URL.
    """
    gc = gspread.service_account(filename=cfg.sheets_credentials_path)
    spreadsheet = gc.open_by_key(cfg.sheets_spreadsheet_id)

    # Rename the default "Sheet1" tab on first use
    sheet1 = spreadsheet.sheet1
    if sheet1.title == "Sheet1":
        sheet1.update_title("All Shows")

    all_ws = _get_or_create_worksheet(spreadsheet, "All Shows", index=0)
    recent_ws = _get_or_create_worksheet(spreadsheet, "Recently Found", index=1)

    recent_shows = [s for s in shows if not s.get("notified")]

    all_ws.clear()
    all_ws.update(_build_rows(shows), value_input_option="RAW")

    recent_ws.clear()
    recent_ws.update(_build_rows(recent_shows), value_input_option="RAW")

    return shorten_url(spreadsheet.url)

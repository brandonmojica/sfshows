from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional


@dataclass
class ShowRecord:
    event_id: str
    source: str
    artist_name: str
    venue_name: str
    venue_city: str
    event_datetime: str   # ISO8601
    ticket_url: Optional[str]
    genre_label: Optional[str]


@dataclass
class CachedGenre:
    artist_name: str
    mbid: Optional[str]
    tags: list[dict]      # [{"name": str, "count": int}]
    genre_label: Optional[str]
    fetched_at: str


SCHEMA = """
CREATE TABLE IF NOT EXISTS shows (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL,
    source         TEXT NOT NULL DEFAULT 'bandsintown',
    artist_name    TEXT NOT NULL,
    venue_name     TEXT NOT NULL,
    venue_city     TEXT NOT NULL,
    event_datetime TEXT NOT NULL,
    ticket_url     TEXT,
    genre_label    TEXT,
    notified       INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(event_id, source)
);

CREATE TABLE IF NOT EXISTS artist_genres (
    artist_name TEXT PRIMARY KEY,
    mbid        TEXT,
    tags_json   TEXT,
    genre_label TEXT,
    fetched_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS run_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at         TEXT NOT NULL DEFAULT (datetime('now')),
    shows_scraped  INTEGER,
    shows_new      INTEGER,
    shows_notified INTEGER,
    error          TEXT
);
"""


class Database:
    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Shows
    # ------------------------------------------------------------------

    def upsert_show(self, show: ShowRecord) -> bool:
        """Insert show. Returns True if it was new (not a duplicate)."""
        try:
            self._conn.execute(
                """
                INSERT INTO shows
                    (event_id, source, artist_name, venue_name, venue_city,
                     event_datetime, ticket_url, genre_label)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    show.event_id,
                    show.source,
                    show.artist_name,
                    show.venue_name,
                    show.venue_city,
                    show.event_datetime,
                    show.ticket_url,
                    show.genre_label,
                ),
            )
            self._conn.commit()
            return True
        except sqlite3.IntegrityError:
            # UNIQUE constraint — already exists
            return False

    def mark_notified(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        placeholders = ",".join("?" * len(event_ids))
        self._conn.execute(
            f"UPDATE shows SET notified = 1 WHERE event_id IN ({placeholders})",
            event_ids,
        )
        self._conn.commit()

    def get_pending_shows(self, source: str = "bandsintown") -> list[ShowRecord]:
        rows = self._conn.execute(
            """
            SELECT event_id, source, artist_name, venue_name, venue_city,
                   event_datetime, ticket_url, genre_label
            FROM shows
            WHERE notified = 0 AND source = ?
            ORDER BY event_datetime ASC
            """,
            (source,),
        ).fetchall()
        return [
            ShowRecord(
                event_id=r["event_id"],
                source=r["source"],
                artist_name=r["artist_name"],
                venue_name=r["venue_name"],
                venue_city=r["venue_city"],
                event_datetime=r["event_datetime"],
                ticket_url=r["ticket_url"],
                genre_label=r["genre_label"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Genre cache
    # ------------------------------------------------------------------

    def get_cached_genre(self, artist_name: str, ttl_days: int) -> Optional[CachedGenre]:
        """Return cached entry if it exists and is not expired. None otherwise."""
        row = self._conn.execute(
            "SELECT * FROM artist_genres WHERE artist_name = ?",
            (artist_name,),
        ).fetchone()
        if row is None:
            return None

        fetched_at = datetime.fromisoformat(row["fetched_at"])
        if datetime.utcnow() - fetched_at > timedelta(days=ttl_days):
            return None

        return CachedGenre(
            artist_name=row["artist_name"],
            mbid=row["mbid"],
            tags=json.loads(row["tags_json"]) if row["tags_json"] else [],
            genre_label=row["genre_label"],
            fetched_at=row["fetched_at"],
        )

    def set_cached_genre(
        self,
        artist_name: str,
        mbid: Optional[str],
        tags: list[dict],
        genre_label: Optional[str],
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO artist_genres (artist_name, mbid, tags_json, genre_label, fetched_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(artist_name) DO UPDATE SET
                mbid = excluded.mbid,
                tags_json = excluded.tags_json,
                genre_label = excluded.genre_label,
                fetched_at = excluded.fetched_at
            """,
            (artist_name, mbid, json.dumps(tags), genre_label),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Run log
    # ------------------------------------------------------------------

    def log_run(
        self,
        scraped: int,
        new: int,
        notified: int,
        error: Optional[str],
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO run_log (shows_scraped, shows_new, shows_notified, error)
            VALUES (?, ?, ?, ?)
            """,
            (scraped, new, notified, error),
        )
        self._conn.commit()

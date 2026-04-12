from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    tags: list[dict]           # [{"name": str, "count": int}]
    genre_label: Optional[str]
    fetched_at: str
    # Extended MusicBrainz fields
    artist_type: Optional[str]   # Person, Group, Orchestra, …
    country: Optional[str]       # ISO 3166-1 alpha-2
    area: Optional[str]          # city/region name
    begin_date: Optional[str]    # life-span begin (YYYY or YYYY-MM-DD)
    end_date: Optional[str]      # life-span end
    ended: Optional[bool]        # True if disbanded/deceased
    mb_genres: list[dict]        # official MB genres [{"name", "count"}]
    rating: Optional[float]      # aggregate user rating 0–5
    rating_votes: Optional[int]
    urls: list[dict]             # [{"type": str, "url": str}]


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
    artist_name   TEXT PRIMARY KEY,
    mbid          TEXT,
    tags_json     TEXT,
    genre_label   TEXT,
    fetched_at    TEXT NOT NULL DEFAULT (datetime('now')),
    artist_type   TEXT,
    country       TEXT,
    area          TEXT,
    begin_date    TEXT,
    end_date      TEXT,
    ended         INTEGER,
    mb_genres_json TEXT,
    rating        REAL,
    rating_votes  INTEGER,
    urls_json     TEXT
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
        self._migrate_artist_genres()

    def _migrate_artist_genres(self) -> None:
        """Add new columns to artist_genres for existing databases."""
        new_columns = [
            ("artist_type",    "TEXT"),
            ("country",        "TEXT"),
            ("area",           "TEXT"),
            ("begin_date",     "TEXT"),
            ("end_date",       "TEXT"),
            ("ended",          "INTEGER"),
            ("mb_genres_json", "TEXT"),
            ("rating",         "REAL"),
            ("rating_votes",   "INTEGER"),
            ("urls_json",      "TEXT"),
        ]
        existing = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(artist_genres)").fetchall()
        }
        for col_name, col_type in new_columns:
            if col_name not in existing:
                self._conn.execute(
                    f"ALTER TABLE artist_genres ADD COLUMN {col_name} {col_type}"
                )
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
    # Artist queries (used by enrichment utilities)
    # ------------------------------------------------------------------

    def artist_in_db(self, artist_name: str) -> bool:
        """Return True if artist exists in artist_genres or shows."""
        in_genres = self._conn.execute(
            "SELECT 1 FROM artist_genres WHERE artist_name = ?", (artist_name,)
        ).fetchone()
        if in_genres:
            return True
        return bool(
            self._conn.execute(
                "SELECT 1 FROM shows WHERE artist_name = ?", (artist_name,)
            ).fetchone()
        )

    def get_artist_mbid(self, artist_name: str) -> Optional[str]:
        """Return cached MBID for artist, or None."""
        row = self._conn.execute(
            "SELECT mbid FROM artist_genres WHERE artist_name = ?", (artist_name,)
        ).fetchone()
        return row[0] if row and row[0] else None

    def get_artists_to_enrich(
        self,
        *,
        force: bool = False,
        missing_only: bool = False,
    ) -> list[str]:
        """
        Return artist names that need enrichment based on mode:
          force=True       — all distinct artists in shows table
          missing_only=True — artists in artist_genres with no mbid
          default          — artists in shows with no artist_type yet
        """
        if force:
            rows = self._conn.execute(
                "SELECT DISTINCT artist_name FROM shows"
            ).fetchall()
        elif missing_only:
            rows = self._conn.execute(
                "SELECT artist_name FROM artist_genres WHERE mbid IS NULL"
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT DISTINCT s.artist_name
                FROM shows s
                LEFT JOIN artist_genres ag ON ag.artist_name = s.artist_name
                WHERE ag.artist_name IS NULL OR ag.artist_type IS NULL
                """
            ).fetchall()
        return [r[0] for r in rows]

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
        if datetime.now(timezone.utc).replace(tzinfo=None) - fetched_at > timedelta(days=ttl_days):
            return None

        return CachedGenre(
            artist_name=row["artist_name"],
            mbid=row["mbid"],
            tags=json.loads(row["tags_json"]) if row["tags_json"] else [],
            genre_label=row["genre_label"],
            fetched_at=row["fetched_at"],
            artist_type=row["artist_type"],
            country=row["country"],
            area=row["area"],
            begin_date=row["begin_date"],
            end_date=row["end_date"],
            ended=bool(row["ended"]) if row["ended"] is not None else None,
            mb_genres=json.loads(row["mb_genres_json"]) if row["mb_genres_json"] else [],
            rating=row["rating"],
            rating_votes=row["rating_votes"],
            urls=json.loads(row["urls_json"]) if row["urls_json"] else [],
        )

    def set_cached_genre(
        self,
        artist_name: str,
        mbid: Optional[str],
        tags: list[dict],
        genre_label: Optional[str],
        *,
        artist_type: Optional[str] = None,
        country: Optional[str] = None,
        area: Optional[str] = None,
        begin_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ended: Optional[bool] = None,
        mb_genres: Optional[list[dict]] = None,
        rating: Optional[float] = None,
        rating_votes: Optional[int] = None,
        urls: Optional[list[dict]] = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO artist_genres
                (artist_name, mbid, tags_json, genre_label, fetched_at,
                 artist_type, country, area, begin_date, end_date, ended,
                 mb_genres_json, rating, rating_votes, urls_json)
            VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(artist_name) DO UPDATE SET
                mbid           = excluded.mbid,
                tags_json      = excluded.tags_json,
                genre_label    = excluded.genre_label,
                fetched_at     = excluded.fetched_at,
                artist_type    = excluded.artist_type,
                country        = excluded.country,
                area           = excluded.area,
                begin_date     = excluded.begin_date,
                end_date       = excluded.end_date,
                ended          = excluded.ended,
                mb_genres_json = excluded.mb_genres_json,
                rating         = excluded.rating,
                rating_votes   = excluded.rating_votes,
                urls_json      = excluded.urls_json
            """,
            (
                artist_name, mbid, json.dumps(tags), genre_label,
                artist_type, country, area, begin_date, end_date,
                int(ended) if ended is not None else None,
                json.dumps(mb_genres or []),
                rating, rating_votes,
                json.dumps(urls or []),
            ),
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

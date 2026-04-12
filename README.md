# sfshows

A personal SF concert scraper and iMessage notifier. Scrapes upcoming shows from Bandsintown venue pages, enriches artists with genre data from MusicBrainz, and sends a formatted digest to your phone via iMessage — automatically, on a daily schedule.

## How it works

1. **Scrape** — Playwright loads each configured Bandsintown venue page, expands hidden dates, and parses artist/date/ticket data from the rendered HTML.
2. **Enrich** — For each unique artist, MusicBrainz is queried for genre tags. Results are cached in SQLite to avoid redundant API calls.
3. **Notify** — Un-notified shows are formatted into a digest and sent via iMessage using AppleScript. Shows are marked as notified so they're never sent twice.

## Requirements

- macOS (iMessage sending relies on AppleScript + Messages.app)
- Python 3.11+
- [Playwright](https://playwright.dev/python/) Chromium browser

## Setup

```bash
# Clone and enter the repo
git clone https://github.com/brandonmojica/sfshows.git
cd sfshows

# Run the setup script — creates .venv, installs deps, installs Playwright,
# and registers a daily cron job (8:00 AM)
./setup_cron.sh
```

The setup script installs a cron entry that runs `run.py` daily and appends output to `~/.sfshows/sfshows.log`. The SQLite database is stored at `~/.sfshows/sfshows.db`.

## Configuration

All settings live in `config.yaml` at the project root.

```yaml
imessage:
  recipients:
    - "+15551234567"       # individual phone number or Apple ID
  # group_name: "SF Shows" # or send to a named group chat (mutually exclusive)

schedule:
  days_ahead: 14           # how many days ahead to show in the digest header

genres:                    # ordered rules — first tag match wins
  - label: "Indie/Alternative"
    tags: ["indie", "alternative", "shoegaze"]
  - label: "Electronic/Dance"
    tags: ["electronic", "techno", "house"]

sources:
  bandsintown: true        # enable/disable scrapers
  billgraham: false

scraper:
  venues:                  # Bandsintown venue page URLs to scrape
    - https://www.bandsintown.com/v/10001698-the-fillmore
    - https://www.bandsintown.com/v/10001685-the-warfield
    # ... add more venues
  venue_concurrency: 5     # parallel venue pages
  user_agent: "Mozilla/5.0 ..."

enrichment:
  min_tag_count: 1         # ignore MusicBrainz tags with fewer than N votes
  request_delay_ms: 1100   # delay between MB API calls (enforces 1 req/sec)
  cache_ttl_days: 30       # re-fetch artist data after N days

database:
  path: "~/.sfshows/sfshows.db"

notification:
  max_shows_per_digest: 20
  include_ticket_url: true
```

### Adding venues

The quickest way to find venue URLs is to browse Bandsintown and copy the venue page URL. You can also run the venue discovery script to scrape all SF venues automatically:

```bash
python scripts/scrape_venues.py   # writes sf_venues.csv to the project root
```

## Usage

```bash
# Activate the virtualenv first
source .venv/bin/activate

# Full run: scrape, enrich, and send iMessage
python run.py

# Preview the digest without sending anything
python run.py --dry-run

# Scrape and store shows; skip notification
python run.py --scrape-only

# Send pending shows already in the DB; skip scraping
python run.py --notify-only

# Save raw HTML from the scraper (useful for debugging selectors)
python run.py --save-html debug.html

# Export scraped shows to CSV
python run.py --csv shows.csv
```

## Digest format

```
SF Shows — Sat Apr 12 – Fri Apr 25
8 new shows

SAT APR 12
• Japanese Breakfast @ The Fillmore, 8:00 PM  [Indie/Alternative]
  tickets: https://www.bandsintown.com/e/...

SUN APR 13
• Four Tet @ 1015 Folsom  [Electronic/Dance]
  tickets: https://www.bandsintown.com/e/...
```

## Utility scripts

### Backfill artist enrichment

Re-enrich artists already in the database without running a full scrape:

```bash
# Enrich artists with no genre data yet (default)
python scripts/enrich_artists.py

# Re-fetch everyone, even already-enriched artists
python scripts/enrich_artists.py --force

# Only artists with no MBID recorded
python scripts/enrich_artists.py --missing-only

# Enrich a single artist by name
python scripts/enrich_artists.py --artist "Radiohead"
```

### Venue discovery

Scrape Bandsintown's SF venue map and export all venue URLs to CSV:

```bash
python scripts/scrape_venues.py   # outputs sf_venues.csv
```

## Project structure

```
sfshows/
├── run.py                        # main entrypoint
├── config.yaml                   # all user configuration
├── setup_cron.sh                 # one-time setup and cron installer
├── requirements.txt
├── scripts/
│   ├── enrich_artists.py         # backfill MusicBrainz artist data
│   └── scrape_venues.py          # discover SF venue URLs
└── sfshows/
    ├── config.py                 # config loader and dataclasses
    ├── db.py                     # SQLite database layer
    ├── digest.py                 # iMessage digest formatter
    ├── notifier.py               # AppleScript iMessage sender
    ├── enrichment/
    │   └── musicbrainz.py        # MusicBrainz genre enrichment
    └── scrapers/
        ├── bandsintown.py        # Playwright-based Bandsintown scraper
        └── billgraham.py         # httpx-based Bill Graham scraper
```

## Database

Three tables are managed automatically in `~/.sfshows/sfshows.db`:

| Table | Purpose |
|---|---|
| `shows` | Every scraped event, with a `notified` flag |
| `artist_genres` | MusicBrainz enrichment cache (TTL-based) |
| `run_log` | One row per pipeline execution with counts and errors |

## Logs

When running via cron, output is appended to `~/.sfshows/sfshows.log`. For manual runs, output goes to stdout.

```bash
tail -f ~/.sfshows/sfshows.log
```

#!/usr/bin/env python3
"""
Scrape all Bandsintown venue listing hrefs for San Francisco.
Output: sf_venues.csv at project root.
Usage (run from project root): python scripts/scrape_venues.py
"""
from __future__ import annotations

import asyncio
import csv
import re
import yaml
from urllib.parse import unquote

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

TARGET_URL = (
    "https://www.bandsintown.com/v"
    "?city_slug=san-francisco-ca"
    "&came_from=252"
    "&utm_medium=web"
    "&utm_source=city_page"
    "&utm_campaign=venue_map_view_all"
)
VENUE_HREF_PREFIX = "https://www.bandsintown.com/v/"
OUTPUT_CSV = "sf_venues.csv"
CONFIG_PATH = "config.yaml"

SCROLL_PAUSE_MS = 2500
MAX_STABLE_ITERS = 3


def load_user_agent(config_path: str = CONFIG_PATH) -> str:
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    return raw["scraper"]["user_agent"]


def venue_name_from_url(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]  # e.g. "10019956-the-midway"
    name_part = re.sub(r"^\d+-", "", slug)  # strip leading numeric ID
    return unquote(name_part).replace("-", " ").title()


def extract_venue_urls(html: str) -> set[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith(VENUE_HREF_PREFIX):
            urls.add(href.split("?")[0])
    return urls


async def scrape_venues() -> list[dict]:
    user_agent = load_user_agent()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent,
            locale="en-US",
        )
        page = await context.new_page()

        print(f"[scrape_venues] Loading {TARGET_URL}")
        await page.goto(TARGET_URL, wait_until="load", timeout=60_000)
        await page.wait_for_timeout(3000)

        all_urls: set[str] = set()
        stable_count = 0

        while stable_count < MAX_STABLE_ITERS:
            html = await page.content()
            new_urls = extract_venue_urls(html)
            added = new_urls - all_urls

            if added:
                all_urls.update(added)
                stable_count = 0
                print(f"[scrape_venues] Found {len(added)} new venues (total: {len(all_urls)})")
            else:
                stable_count += 1
                print(f"[scrape_venues] No new venues (stable {stable_count}/{MAX_STABLE_ITERS})")

            if stable_count >= MAX_STABLE_ITERS:
                break

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(SCROLL_PAUSE_MS)

        await browser.close()

    print(f"[scrape_venues] Scrape complete. {len(all_urls)} unique venues found.")
    return [
        {"venue_url": url, "venue_name": venue_name_from_url(url)}
        for url in sorted(all_urls)
    ]


def write_csv(rows: list[dict], path: str = OUTPUT_CSV) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["venue_url", "venue_name"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"[scrape_venues] Wrote {len(rows)} venues to {path}")


async def main() -> None:
    rows = await scrape_venues()
    write_csv(rows)


if __name__ == "__main__":
    asyncio.run(main())

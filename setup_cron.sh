#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
LOG_DIR="$HOME/.sfshows"
LOG_FILE="$LOG_DIR/sfshows.log"

echo "=== sfshows setup ==="
echo "Project: $SCRIPT_DIR"
echo "Venv:    $VENV_DIR"
echo ""

# Create .venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "[1/4] Creating .venv..."
    python3 -m venv "$VENV_DIR"
else
    echo "[1/4] .venv already exists — skipping"
fi

# Create log/DB directory
mkdir -p "$LOG_DIR"

# Install Python dependencies into venv
echo "[2/4] Installing Python dependencies..."
"$PYTHON" -m pip install -r "$SCRIPT_DIR/requirements.txt" --quiet
echo "[2/4] Dependencies installed"

# Install Playwright Chromium browser
echo "[3/4] Installing Playwright Chromium..."
"$PYTHON" -m playwright install chromium
echo "[3/4] Playwright Chromium installed"

# Install cron job (daily at 8 AM)
CRON_MARKER="sfshows"
CRON_LINE="0 8 * * * cd '$SCRIPT_DIR' && '$PYTHON' run.py >> '$LOG_FILE' 2>&1"

if crontab -l 2>/dev/null | grep -qF "$CRON_MARKER"; then
    echo "[4/4] Cron entry already exists — skipping (remove it manually to reinstall)"
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "[4/4] Cron job installed: runs daily at 8:00 AM"
fi

echo ""
echo "Setup complete."
echo ""
echo "Activate the venv for manual runs:"
echo "  source .venv/bin/activate"
echo ""
echo "Test your setup:"
echo "  python3 run.py --dry-run      # Scrape and preview digest, no iMessage sent"
echo "  python3 run.py --scrape-only  # Scrape and store shows only"
echo "  python3 run.py                # Full run: scrape + notify"
echo ""
echo "Logs: $LOG_FILE"

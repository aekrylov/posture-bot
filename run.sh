#!/bin/sh
# Loads .env (BOT_TOKEN=..., optional CAPTION=...) and runs the bot.
cd "$(dirname "$0")" || exit 1
set -a; [ -f .env ] && . ./.env; set +a
exec /usr/bin/env python3 -u bot.py

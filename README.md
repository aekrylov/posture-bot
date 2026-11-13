# posture-bot

Telegram bot that sends `photo.jpg` to every subscriber once an hour, at a random second within the hour. It uses only the Python 3.9+ standard library.

## Commands

- `/start`: subscribe
- `/stop`: unsubscribe
- `/next`: show when the next photo goes out

Users who block the bot or remove it from a chat are deactivated automatically.

## Run

```sh
cp .env.example .env && chmod 600 .env   # put the BotFather token in it
./run.sh
```

### Run under launchd (macOS)

The bot starts at login and restarts if it crashes.

```sh
cp deploy/org.anth.posture-bot.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.anth.posture-bot.plist
tail -f bot.log
```

### Run under systemd (Linux)

See `deploy/posture-bot.service`.

## Outage handling

Telegram's server-side scheduled messages aren't available to bots. The Bot API has no schedule parameter, and MTProto rejects bot scheduling with `SCHEDULE_BOT_NOT_ALLOWED`. The bot therefore keeps its own durable schedule:

- **Deterministic slots.** Each hour's send second is `HMAC(seed, hour) mod 3600`, and the seed is stored in SQLite. A restart therefore never re-rolls the time, skips a slot, or sends a slot twice.
- **Delivery log.** Every successful send is recorded per (slot, chat). A broadcast interrupted by a crash resumes with the users who haven't received it yet.
- **No late spam.** If the bot is down or the host is asleep when a slot fires, the slot is only delivered if the bot is back within `CATCHUP_GRACE` (default 2 min). Anything later is dropped, and the next hour goes out as usual.
- **Wall-clock checks.** The scheduler re-checks the clock at least every 30 s and never sleeps for an hour at a time, so it notices system sleep and wake.
- **Supervisor.** launchd or systemd restarts the process. A file lock prevents two instances from running against the same database.
- **Rate limits.** Sends are throttled to 25 msg/s, and `retry_after` from a 429 response is honoured. The photo is uploaded once, then sent by `file_id`.

## Test

```sh
python3 test_e2e.py
```

The test runs against a fake Bot API with 6-second "hours", so no token is needed. It checks:

- one delivery per slot, never early
- SIGKILL outages: a slot missed during the outage is caught up when the bot restarts within the catch-up window, and skipped when it restarts after the window
- blocked users are deactivated
- `/stop` unsubscribes
- the photo is uploaded once, then reused by `file_id`
- a second instance refuses to start

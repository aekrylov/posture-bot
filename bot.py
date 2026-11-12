#!/usr/bin/env python3
"""Posture bot: sends photo.jpg to every active subscriber once per hour,
at a random second within that hour.

Stdlib only (Python 3.9+). Config via environment:
  BOT_TOKEN       required
  PHOTO_PATH      default ./photo.jpg
  DB_PATH         default ./bot.sqlite3
  CATCHUP_GRACE   seconds a missed slot may still be delivered late (default 1800)
  SEND_RATE       max messages/second during a broadcast (default 25; TG cap ~30)
  CAPTION         optional caption
  API_BASE        default https://api.telegram.org (override for tests)
  PERIOD          slot length in seconds (default 3600; override for tests)

Why there is no Telegram-side scheduling: the Bot API has no schedule
parameter, and MTProto rejects bot scheduling with SCHEDULE_BOT_NOT_ALLOWED.
Outage tolerance therefore comes from:
  * a deterministic schedule: slot time = HMAC(seed, hour) mod PERIOD, with the
    seed persisted, so restarts never re-roll the second or skip/duplicate;
  * a persisted per-(slot, chat) delivery log, so a crash mid-broadcast resumes
    exactly where it stopped;
  * catch-up: a slot missed while the bot/host was down (or the Mac asleep) is
    still delivered if we're back within CATCHUP_GRACE; older ones are dropped;
  * wall-clock polling (never one long sleep), so host sleep/wake is handled;
  * launchd/systemd KeepAlive to restart the process (see deploy/).
"""
from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import logging
import math
import os
import secrets
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
TOKEN = os.environ.get("BOT_TOKEN", "")
API_BASE = os.environ.get("API_BASE", "https://api.telegram.org").rstrip("/")
PHOTO_PATH = os.environ.get("PHOTO_PATH", os.path.join(HERE, "photo.jpg"))
DB_PATH = os.environ.get("DB_PATH", os.path.join(HERE, "bot.sqlite3"))
GRACE = int(os.environ.get("CATCHUP_GRACE", "1800"))
SEND_RATE = float(os.environ.get("SEND_RATE", "25"))
CAPTION = os.environ.get("CAPTION", "")
PERIOD = int(os.environ.get("PERIOD", "3600"))
MAX_IDLE = min(30, max(1, PERIOD // 4))  # max sleep between schedule checks

log = logging.getLogger("posture-bot")
stop_event = threading.Event()


# ---------------------------------------------------------------- Telegram API
class ApiError(Exception):
    def __init__(self, code: int, desc: str, retry_after: int | None = None):
        super().__init__(f"{code}: {desc}")
        self.code, self.desc, self.retry_after = code, desc, retry_after


def _multipart(fields: dict, files: dict) -> tuple[bytes, str]:
    boundary = uuid.uuid4().hex
    out = []
    for k, v in fields.items():
        out += [f"--{boundary}\r\n", f'Content-Disposition: form-data; name="{k}"\r\n\r\n', f"{v}\r\n"]
    body = "".join(out).encode()
    for k, (fname, data, ctype) in files.items():
        body += (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"; "
                 f"filename=\"{fname}\"\r\nContent-Type: {ctype}\r\n\r\n").encode() + data + b"\r\n"
    body += f"--{boundary}--\r\n".encode()
    return body, f"multipart/form-data; boundary={boundary}"


def api(method: str, params: dict | None = None, files: dict | None = None, timeout: float = 30):
    url = f"{API_BASE}/bot{TOKEN}/{method}"
    params = {k: v for k, v in (params or {}).items() if v is not None}
    if files:
        fields = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in params.items()}
        body, ctype = _multipart(fields, files)
    else:
        body, ctype = json.dumps(params).encode(), "application/json"
    req = urllib.request.Request(url, data=body, headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.load(r)
    except urllib.error.HTTPError as e:
        try:
            data = json.load(e)
        except Exception:
            raise ApiError(e.code, str(e)) from None
    if not data.get("ok"):
        ra = (data.get("parameters") or {}).get("retry_after")
        raise ApiError(data.get("error_code", 0), data.get("description", ""), ra)
    return data["result"]


# ---------------------------------------------------------------- storage
def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    return c


def init_db() -> None:
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(
        chat_id INTEGER PRIMARY KEY, active INTEGER NOT NULL,
        subscribed_at INTEGER NOT NULL, name TEXT);
    CREATE TABLE IF NOT EXISTS sent(
        slot INTEGER NOT NULL, chat_id INTEGER NOT NULL, ts INTEGER NOT NULL,
        PRIMARY KEY(slot, chat_id));
    CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
    """)
    c.execute("INSERT OR IGNORE INTO kv VALUES('seed', ?)", (secrets.token_hex(32),))
    c.close()


def kv_get(c, k, default=None):
    row = c.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return row[0] if row else default


def kv_set(c, k, v):
    c.execute("INSERT INTO kv VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


# ---------------------------------------------------------------- schedule
_seed: bytes | None = None


def slot_time(slot: int) -> int:
    """Unix time at which slot (= hour index) fires: slot*PERIOD + random second."""
    global _seed
    if _seed is None:
        c = db(); _seed = bytes.fromhex(kv_get(c, "seed")); c.close()
    h = hmac.new(_seed, str(slot).encode(), hashlib.sha256).digest()
    return slot * PERIOD + int.from_bytes(h[:8], "big") % PERIOD


def due_slots(now: float) -> list[int]:
    """Slots whose time has passed but are still within the catch-up window."""
    first = int((now - GRACE) // PERIOD)
    return [s for s in range(first, int(now // PERIOD) + 1) if now - GRACE <= slot_time(s) <= now]


def next_slot_time(now: float) -> int:
    s = int(now // PERIOD)
    while slot_time(s) <= now:
        s += 1
    return slot_time(s)


def fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


# ---------------------------------------------------------------- sending
_photo_lock = threading.Lock()


def send_photo(c, chat_id: int) -> None:
    """Upload once, then reuse Telegram's file_id (re-upload if the photo changes)."""
    with open(PHOTO_PATH, "rb") as f:
        data = f.read()
    digest = hashlib.sha256(data).hexdigest()
    with _photo_lock:
        fid = kv_get(c, "file_id") if kv_get(c, "file_sha") == digest else None
        if fid:
            try:
                api("sendPhoto", {"chat_id": chat_id, "photo": fid, "caption": CAPTION or None})
                return
            except ApiError as e:
                if not (e.code == 400 and "file" in e.desc.lower()):
                    raise
                log.warning("cached file_id rejected (%s); re-uploading", e.desc)
        res = api("sendPhoto", {"chat_id": chat_id, "caption": CAPTION or None},
                  files={"photo": (os.path.basename(PHOTO_PATH), data, "image/jpeg")}, timeout=120)
        kv_set(c, "file_id", res["photo"][-1]["file_id"])
        kv_set(c, "file_sha", digest)


def broadcast(slot: int) -> None:
    c = db()
    t = slot_time(slot)
    # only users subscribed before the slot fired; skip ones already served
    pending = [r[0] for r in c.execute(
        "SELECT chat_id FROM users u WHERE active=1 AND subscribed_at<=? AND NOT EXISTS"
        " (SELECT 1 FROM sent s WHERE s.slot=? AND s.chat_id=u.chat_id)", (t, slot))]
    if pending:
        log.info("slot %d (%s): sending to %d users", slot, fmt(t), len(pending))
    for chat_id in pending:
        if stop_event.is_set() or time.time() - t > GRACE:
            break
        for attempt in range(5):
            try:
                send_photo(c, chat_id)
                c.execute("INSERT OR IGNORE INTO sent VALUES(?,?,?)", (slot, chat_id, int(time.time())))
                break
            except ApiError as e:
                if e.retry_after:
                    log.warning("rate limited, sleeping %ss", e.retry_after)
                    time.sleep(e.retry_after + 1)
                    continue
                if e.code == 403 or "chat not found" in e.desc.lower():
                    log.info("deactivating %s: %s", chat_id, e.desc)
                    c.execute("UPDATE users SET active=0 WHERE chat_id=?", (chat_id,))
                    break
                log.error("send to %s failed: %s", chat_id, e)  # retried on next pass
                break
            except (urllib.error.URLError, OSError) as e:
                log.error("network error sending to %s: %s", chat_id, e)
                time.sleep(min(2 ** attempt, 30))
        time.sleep(1 / SEND_RATE)
    # prune old delivery log
    c.execute("DELETE FROM sent WHERE slot < ?", (slot - 48 * 3600 // PERIOD,))
    c.close()


def scheduler() -> None:
    while not stop_event.is_set():
        now = time.time()
        for s in due_slots(now):
            try:
                broadcast(s)
            except Exception:
                log.exception("broadcast %d crashed", s)
        wait = min(next_slot_time(time.time()) - time.time(), MAX_IDLE)
        stop_event.wait(max(wait, 0.2))


# ---------------------------------------------------------------- commands
def reply(chat_id: int, text: str) -> None:
    try:
        api("sendMessage", {"chat_id": chat_id, "text": text})
    except Exception as e:
        log.warning("reply to %s failed: %s", chat_id, e)


def send_now(c, chat_id: int) -> None:
    for _ in range(3):
        try:
            send_photo(c, chat_id)
            return
        except ApiError as e:
            if e.retry_after:
                time.sleep(e.retry_after + 1)
                continue
            if e.code == 403 or "chat not found" in e.desc.lower():
                c.execute("UPDATE users SET active=0 WHERE chat_id=?", (chat_id,))
            log.warning("welcome photo to %s failed: %s", chat_id, e)
            return
        except (urllib.error.URLError, OSError) as e:
            log.warning("welcome photo to %s failed: %s", chat_id, e)
            time.sleep(2)


def handle(c, upd: dict) -> None:
    mcm = upd.get("my_chat_member")
    if mcm:  # bot blocked / kicked / re-added
        status = mcm["new_chat_member"]["status"]
        if status in ("kicked", "left"):
            c.execute("UPDATE users SET active=0 WHERE chat_id=?", (mcm["chat"]["id"],))
        return
    msg = upd.get("message") or upd.get("channel_post")
    if not msg or "text" not in msg:
        return
    cmd = msg["text"].split()[0].split("@")[0].lower()
    chat = msg["chat"]
    cid = chat["id"]
    name = chat.get("title") or chat.get("username") or chat.get("first_name")
    if cmd == "/start":
        c.execute("INSERT INTO users VALUES(?,1,?,?) ON CONFLICT(chat_id) DO UPDATE SET "
                  "active=1, subscribed_at=excluded.subscribed_at, name=excluded.name",
                  (cid, math.ceil(time.time()), name))
        reply(cid, "Subscribed: one reminder every hour at a random second.\n"
                   f"Next one: {fmt(next_slot_time(time.time()))}\n/stop to unsubscribe.")
        send_now(c, cid)  # first one right away; not counted as an hourly slot
    elif cmd == "/stop":
        c.execute("UPDATE users SET active=0 WHERE chat_id=?", (cid,))
        reply(cid, "Unsubscribed. /start to resume.")
    elif cmd == "/next":
        reply(cid, f"Next reminder: {fmt(next_slot_time(time.time()))}")


def poller() -> None:
    c = db()
    offset = int(kv_get(c, "offset", 0))
    backoff = 1
    while not stop_event.is_set():
        try:
            updates = api("getUpdates", {"offset": offset, "timeout": 25,
                                         "allowed_updates": ["message", "channel_post", "my_chat_member"]},
                          timeout=35)
            backoff = 1
        except Exception as e:
            log.warning("getUpdates failed: %s", e)
            stop_event.wait(backoff)
            backoff = min(backoff * 2, 60)
            continue
        for u in updates:
            try:
                handle(c, u)
            except Exception:
                log.exception("update %s failed", u.get("update_id"))
            offset = u["update_id"] + 1
            kv_set(c, "offset", offset)


# ---------------------------------------------------------------- main
def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
    if not TOKEN:
        sys.exit("BOT_TOKEN is not set")
    if not os.path.exists(PHOTO_PATH):
        sys.exit(f"photo not found: {PHOTO_PATH}")
    lock = open(DB_PATH + ".lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)  # one instance per DB
    except BlockingIOError:
        sys.exit("another instance is already running")
    init_db()
    me = api("getMe")
    log.info("running as @%s; next slot %s", me.get("username"), fmt(next_slot_time(time.time())))
    threads = [threading.Thread(target=f, name=f.__name__, daemon=True) for f in (poller, scheduler)]
    for t in threads:
        t.start()
    try:
        while all(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        sys.exit(0)
    stop_event.set()
    log.error("a worker thread died; exiting so the supervisor restarts us")
    sys.exit(1)


if __name__ == "__main__":
    main()

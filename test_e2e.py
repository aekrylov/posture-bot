#!/usr/bin/env python3
"""End-to-end test against a fake Telegram Bot API (no real token needed).

Runs the bot with PERIOD=6s so an "hour" is 6 seconds, kills it with SIGKILL
mid-run to simulate an outage, restarts it, and checks:
  * every active user gets exactly one photo per slot, never before slot time
  * a slot missed during the outage is caught up after restart (within grace)
  * a user who blocked the bot (403) is deactivated and not retried
  * /start sends a photo immediately; /stop unsubscribes; the photo is uploaded once, then sent by file_id
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
BLOCKED = 666
lock = threading.Lock()
updates, photos, messages = [], [], []
uid = [0]


def push(chat_id, text):
    with lock:
        uid[0] += 1
        updates.append({"update_id": uid[0], "message": {
            "message_id": uid[0], "text": text,
            "chat": {"id": chat_id, "type": "private", "first_name": f"u{chat_id}"}}})


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        method = self.path.rsplit("/", 1)[-1]
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        ctype = self.headers.get("Content-Type", "")
        if ctype.startswith("application/json"):
            p = json.loads(body or b"{}")
            upload = False
        else:  # multipart: crude field extraction is enough here
            upload = b'name="photo"; filename=' in body
            p = {"chat_id": int(body.split(b'name="chat_id"\r\n\r\n')[1].split(b"\r\n")[0])}
        if method == "getMe":
            return self.reply({"ok": True, "result": {"id": 1, "username": "fake_bot"}})
        if method == "getUpdates":
            deadline = time.time() + min(p.get("timeout", 0), 1)
            while time.time() < deadline:
                with lock:
                    out = [u for u in updates if u["update_id"] >= p.get("offset", 0)]
                if out:
                    break
                time.sleep(0.05)
            return self.reply({"ok": True, "result": out})
        if method == "sendMessage":
            with lock:
                messages.append((p["chat_id"], p["text"]))
            return self.reply({"ok": True, "result": {}})
        if method == "sendPhoto":
            cid = int(p["chat_id"])
            if cid == BLOCKED:
                with lock:
                    photos.append((cid, time.time(), "blocked"))
                return self.reply({"ok": False, "error_code": 403,
                                   "description": "Forbidden: bot was blocked by the user"}, 403)
            kind = "upload" if upload else ("file_id" if p.get("photo") == "FID" else "bad")
            with lock:
                photos.append((cid, time.time(), kind))
            return self.reply({"ok": True, "result": {"photo": [{"file_id": "small"}, {"file_id": "FID"}]}})
        self.reply({"ok": False, "error_code": 404, "description": "Not Found"}, 404)


def main():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "bot.sqlite3")
    PERIOD, GRACE = 6, 4
    env = dict(os.environ, BOT_TOKEN="TEST", API_BASE=f"http://127.0.0.1:{srv.server_port}",
               DB_PATH=db_path, PERIOD=str(PERIOD), CATCHUP_GRACE=str(GRACE),
               PHOTO_PATH=os.path.join(HERE, "photo.jpg"), SEND_RATE="100")

    def start():
        return subprocess.Popen([sys.executable, os.path.join(HERE, "bot.py")], env=env,
                                stdout=open(os.path.join(tmp, "log.txt"), "a"), stderr=subprocess.STDOUT)

    t0 = time.time()
    bot = start()
    time.sleep(1)
    for cid in (1, 2, BLOCKED):
        push(cid, "/start")
    time.sleep(1.5)
    sub_time = time.time()

    # second instance must refuse to run (single-instance lock)
    dup = subprocess.run([sys.executable, os.path.join(HERE, "bot.py")], env=env, capture_output=True, text=True)
    assert "already running" in dup.stderr, dup.stderr

    time.sleep(PERIOD * 2)
    push(2, "/stop")
    time.sleep(1.5)
    stop_time = time.time()

    import importlib.util
    os.environ.update(env)
    spec = importlib.util.spec_from_file_location("bot", os.path.join(HERE, "bot.py"))
    b = importlib.util.module_from_spec(spec); spec.loader.exec_module(b)

    outages = []

    def outage(extra):
        """SIGKILL the bot, stay down until (next slot + extra), restart."""
        nonlocal bot
        bot.send_signal(signal.SIGKILL); bot.wait()
        start_down = time.time()
        time.sleep(b.next_slot_time(start_down) + extra - time.time())
        outages.append((start_down, time.time()))
        bot = start()

    outage(2)           # slot missed, but restart within grace -> must be caught up
    time.sleep(PERIOD * 1.5)
    outage(GRACE + 1)   # down past grace -> stale slot must be skipped
    time.sleep(PERIOD * 1.5)
    bot.send_signal(signal.SIGINT); bot.wait(10)
    t_end = time.time()

    c = sqlite3.connect(db_path)
    sent = c.execute("SELECT slot, chat_id, ts FROM sent ORDER BY slot").fetchall()
    users = dict(c.execute("SELECT chat_id, active FROM users"))
    sub_time = c.execute("SELECT subscribed_at FROM users WHERE chat_id=1").fetchone()[0]
    print("users:", users)
    print("sent:", sent)
    print("photos:", [(cid, round(t - t0, 2), k) for cid, t, k in photos])
    print("messages:", messages)

    assert users == {1: 1, 2: 0, BLOCKED: 0}, users
    assert len([p for p in photos if p[0] == BLOCKED]) == 1, "blocked user retried"
    kinds = [k for cid, _, k in photos if cid != BLOCKED]
    assert kinds[0] == "upload" and all(k == "file_id" for k in kinds[1:]), kinds

    # expected slots for user 1: all slots fired after subscribing, until shutdown,
    # except those whose catch-up window expired entirely during the outage
    slots = range(int(sub_time // PERIOD), int(t_end // PERIOD) + 1)
    def skipped(st):
        return any(a < st and st + GRACE < z for a, z in outages)

    def in_outage(st):
        return any(a < st < z for a, z in outages)

    exp1 = [s for s in slots if sub_time <= b.slot_time(s) < t_end - 1.5 and not skipped(b.slot_time(s))]
    assert any(in_outage(b.slot_time(s)) for s in exp1), "no caught-up slot exercised"
    assert any(skipped(b.slot_time(s)) for s in slots), "no skipped slot exercised"
    got1 = [s for s, cid, _ in sent if cid == 1 and b.slot_time(s) < t_end - 1.5]  # ignore shutdown race
    print("user1 expected", exp1, "got", got1)
    assert got1 == exp1, (exp1, got1)
    assert len(got1) == len(set(got1)), "duplicate delivery"
    got2 = [s for s, cid, _ in sent if cid == 2]
    assert all(b.slot_time(s) < stop_time for s in got2) and got2, got2
    # immediate photo on /start for each subscriber
    subs = dict(c.execute("SELECT chat_id, subscribed_at FROM users"))
    welcome = {}
    for cid in (1, 2, BLOCKED):
        w = [t for pc, t, _ in photos if pc == cid and abs(t - subs[cid]) < 2]
        assert w, f"no welcome photo for {cid}"
        welcome[cid] = w[0]
        print(f"user {cid}: welcome photo {w[0] - t0:.2f}s after test start")
    # never early, and on time unless caught up after the outage
    for cid, t, k in photos:
        if cid != 1 or t == welcome[1]:
            continue
        s = int(t // PERIOD) if b.slot_time(int(t // PERIOD)) <= t else int(t // PERIOD) - 1
        assert t >= b.slot_time(s), "sent early"
        lag = t - b.slot_time(s)
        caught_up = in_outage(b.slot_time(s))
        assert lag < (GRACE if caught_up else 1.0), (s, lag)
        print(f"slot {s}: lag {lag:.2f}s{' (caught up after outage)' if caught_up else ''}")
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()

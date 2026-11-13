#!/usr/bin/env python3
"""Local test of the serverless handlers: moto fakes DynamoDB/SSM/Scheduler,
a fake Telegram API (from ../test_e2e.py) records sends. PERIOD=6s, GRACE=4s.

Run: <venv with moto>/bin/python test_local.py
"""
import json
import os
import sys
import threading
import time
from http.server import ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "src"))
import test_e2e as tg  # noqa: E402  fake Telegram server

calls = []


class Fake(tg.H):
    def do_POST(self):
        method = self.path.rsplit("/", 1)[-1]
        if method == "setWebhook":
            p = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(("setWebhook", p))
            return self.reply({"ok": True, "result": True})
        return super().do_POST()


srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
threading.Thread(target=srv.serve_forever, daemon=True).start()
os.environ.update({
    "AWS_DEFAULT_REGION": "eu-central-1", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
    "USERS_TABLE": "users", "STATE_TABLE": "state", "TOKEN_PARAM": "/posture-bot/token",
    "API_BASE": f"http://127.0.0.1:{srv.server_port}", "PERIOD": "6", "CATCHUP_GRACE": "4",
    "LOOKAHEAD": "40", "SCHEDULE_GROUP": "posture-bot",
    "SCHEDULER_ROLE_ARN": "arn:aws:iam::123456789012:role/sched",
    "BROADCAST_ARN": "arn:aws:lambda:eu-central-1:123456789012:function:posture-bot-broadcast",
    "DLQ_ARN": "arn:aws:sqs:eu-central-1:123456789012:dlq",
    "ALLOWED_CHANNELS": "@GoodChan, -1003",
})

from moto import mock_aws  # noqa: E402

ok = lambda m: print("PASS", m)  # noqa: E731


class Ctx:
    def __init__(self, budget=(900_000,)):
        self.budget = list(budget)

    def get_remaining_time_in_millis(self):
        return self.budget.pop(0) if len(self.budget) > 1 else self.budget[0]


def update(uid, chat_id, text):
    return {"update_id": uid, "message": {"message_id": uid, "text": text,
                                          "chat": {"id": chat_id, "type": "private", "first_name": "x"}}}


with mock_aws():
    import boto3
    ddb = boto3.client("dynamodb")
    ddb.create_table(TableName="users", BillingMode="PAY_PER_REQUEST",
                     AttributeDefinitions=[{"AttributeName": "chat_id", "AttributeType": "N"}],
                     KeySchema=[{"AttributeName": "chat_id", "KeyType": "HASH"}])
    ddb.create_table(TableName="state", BillingMode="PAY_PER_REQUEST",
                     AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                           {"AttributeName": "sk", "AttributeType": "S"}],
                     KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                                {"AttributeName": "sk", "KeyType": "RANGE"}])
    boto3.client("ssm").put_parameter(Name="/posture-bot/token", Value="TEST", Type="SecureString")
    sched = boto3.client("scheduler")
    sched.create_schedule_group(Name="posture-bot")

    import app
    invoked = []
    app.lambda_.invoke = lambda **kw: invoked.append(json.loads(kw["Payload"])["slot"])
    secret = app.webhook_secret()

    def hook(upd, s=secret):
        return app.webhook({"headers": {"X-Telegram-Bot-Api-Secret-Token": s}, "body": json.dumps(upd)}, Ctx())

    def photos_to(cid, since=0):
        return [t for c, t, k in tg.photos if c == cid and t >= since]

    # --- webhook
    assert hook(update(1, 1, "/start"), "wrong")["statusCode"] == 403
    ok("webhook rejects bad secret")
    t0 = time.time()
    for i, cid in enumerate((1, 2, tg.BLOCKED), start=10):
        assert hook(update(i, cid, "/start"))["statusCode"] == 200
    assert all(photos_to(c, t0) for c in (1, 2, tg.BLOCKED)), tg.photos
    u = {int(i["chat_id"]): int(i["active"]) for i in app.users.scan()["Items"]}
    assert u == {1: 1, 2: 1, tg.BLOCKED: 0}, u
    ok("/start replies + sends photo immediately; blocked user deactivated")
    n = len(tg.photos)
    assert hook(update(10, 1, "/start"))["body"] == "dup" and len(tg.photos) == n
    ok("duplicate update_id ignored")
    kinds = [k for c, _, k in tg.photos if c != tg.BLOCKED]
    assert kinds[0] == "upload" and set(kinds[1:]) == {"file_id"}, kinds
    ok("photo uploaded once, then sent by file_id")

    # --- channels: subscribe by granting the bot "Post messages" admin rights
    def mcm(uid, chat, status, **rights):
        return {"update_id": uid, "my_chat_member": {"chat": chat, "new_chat_member": {"status": status, **rights}}}

    def active(cid):
        return int((app.users.get_item(Key={"chat_id": cid}).get("Item") or {}).get("active", 0))

    CH = {"id": -1001, "type": "channel", "title": "Good", "username": "goodchan"}
    CH3 = {"id": -1003, "type": "channel", "title": "Private by id"}
    EVIL = {"id": -1002, "type": "channel", "title": "Other", "username": "other"}
    nmsg = len(tg.messages)
    hook(mcm(30, CH, "administrator", can_post_messages=True))
    assert active(-1001) == 1 and len(photos_to(-1001, t0)) == 1
    hook(mcm(31, CH, "administrator", can_post_messages=True, can_edit_messages=True))
    assert len(photos_to(-1001, t0)) == 1
    ok("channel subscribed on admin+post rights, first photo posted once (rights change doesn't repost)")
    hook(mcm(32, EVIL, "administrator", can_post_messages=True))
    assert active(-1002) == 0 and not photos_to(-1002, t0)
    hook(mcm(33, CH3, "administrator", can_post_messages=False))
    assert active(-1003) == 0 and not photos_to(-1003, t0)
    hook({"update_id": 34, "channel_post": {"message_id": 1, "text": "/start", "chat": EVIL}})
    assert active(-1002) == 0 and len(tg.messages) == nmsg
    ok("allowlist enforced; admin without post rights ignored; channel posts / text replies never sent to channels")
    hook(mcm(35, CH3, "administrator", can_post_messages=True))
    assert active(-1003) == 1 and photos_to(-1003, t0)
    hook(mcm(36, CH3, "administrator", can_post_messages=False))
    assert active(-1003) == 0
    ok("allowlist by numeric id works; revoking post rights unsubscribes")

    # --- planner creates one-time schedules
    res = app.planner({}, Ctx())
    names = {s["Name"] for s in sched.list_schedules(GroupName="posture-bot")["Schedules"]}
    assert res["created"] and names == {f"slot-{s}" for s in res["created"]}, (res, names)
    s0 = res["created"][0]
    d = sched.get_schedule(GroupName="posture-bot", Name=f"slot-{s0}")
    t = app.slot_time(s0)
    from datetime import datetime, timezone
    assert d["ScheduleExpression"] == f"at({datetime.fromtimestamp(t - t % 60, timezone.utc):%Y-%m-%dT%H:%M:%S})"
    assert json.loads(d["Target"]["Input"]) == {"slot": s0} and d["ActionAfterCompletion"] == "DELETE"
    assert d["Target"]["RetryPolicy"]["MaximumEventAgeInSeconds"] == 60  # GRACE clamped to >= 60
    ok(f"planner created {len(res['created'])} at() schedules (minute-floored), invoked {res['invoked']}")
    assert app.planner({}, Ctx())["created"] == []
    ok("planner is idempotent")

    # --- broadcast at the exact second
    s = next(x for x in range(int(time.time() // 6), 10**12) if app.slot_time(x) > time.time() + 1.5)
    t = app.slot_time(s)
    r = app.broadcast({"slot": s}, Ctx())
    got = {c: photos_to(c, t) for c in (1, 2)}
    got[-1001] = photos_to(-1001, t)
    assert r == {"sent": 3, "failed": 0} and all(got.values()), (r, got)
    lag = max(v[0] for v in got.values()) - t
    assert 0 <= min(v[0] for v in got.values()) - t and lag < 1.0, lag
    ok(f"broadcast slot {s}: 2 users + channel, lag {lag:.3f}s")
    assert app.broadcast({"slot": s}, Ctx()) == {"skipped": "done"}
    ok("re-run of a done slot sends nothing")

    # --- lease
    s2 = s + 1
    app.state.put_item(Item={"pk": "lock", "sk": str(s2), "until": int(time.time()) + 60})
    time.sleep(max(0, app.slot_time(s2) - time.time()))
    assert app.broadcast({"slot": s2}, Ctx()) == {"skipped": "locked"}
    app.state.delete_item(Key={"pk": "lock", "sk": str(s2)})
    ok("concurrent run blocked by lease")

    # --- continuation: run out of Lambda time after first send, resume
    invoked.clear()
    before = time.time()
    r1 = app.broadcast({"slot": s2}, Ctx([900_000, 900_000, 900_000, 10_000]))
    assert r1.get("continued") and r1["sent"] == 1 and invoked == [s2], (r1, invoked)
    assert "Item" not in app.state.get_item(Key={"pk": "lock", "sk": str(s2)})
    r2 = app.broadcast({"slot": s2}, Ctx())
    assert r2 == {"sent": 2, "failed": 0}, r2
    assert all(len(photos_to(c, before)) == 1 for c in (1, 2, -1001))
    ok("low-time continuation resumes without duplicates")

    # --- stale slot skipped
    assert app.broadcast({"slot": s - 3}, Ctx()) == {"skipped": "stale"}
    ok("slot older than grace skipped")

    # --- planner catch-up for a missed slot (schedule lost, not done)
    s3 = s2 + 1
    time.sleep(max(0, app.slot_time(s3) - time.time()) + 0.5)
    try:
        sched.delete_schedule(GroupName="posture-bot", Name=f"slot-{s3}")
    except Exception:
        pass
    invoked.clear()
    assert s3 in app.planner({}, Ctx())["invoked"] and s3 in invoked
    ok("planner invokes overdue undelivered slot within grace")

    # --- /stop, kicked, /next, set_webhook
    hook(update(20, 2, "/stop"))
    hook({"update_id": 21, "my_chat_member": {"chat": {"id": 1}, "new_chat_member": {"status": "kicked"}}})
    u = {int(i["chat_id"]): int(i["active"]) for i in app.users.scan()["Items"] if int(i["chat_id"]) > 0}
    assert u == {1: 0, 2: 0, tg.BLOCKED: 0}, u
    hook(mcm(23, CH, "left"))
    assert active(-1001) == 0
    ok("/stop, bot-kicked and removed-from-channel deactivate")
    hook(update(22, 2, "/next"))
    assert tg.messages[-1][1].startswith("Next reminder:"), tg.messages[-1]
    r = app.planner({"set_webhook": "https://example.lambda-url.on.aws/"}, Ctx())
    assert calls[-1][1]["secret_token"] == secret and calls[-1][1]["url"].startswith("https://"), calls
    ok("set_webhook registers URL with secret token")
    print("ALL CHECKS PASSED")

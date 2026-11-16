#!/usr/bin/env python3
"""Local test of the multi-bot serverless handlers.

moto fakes DynamoDB / SSM / Scheduler; a fake Telegram Bot API (multi-token)
records every call. PERIOD=6 s, GRACE=4 s.

Run: <venv with moto + boto3>/bin/python test_local.py
"""
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "src"))

# ------------------------------------------------------------ fake Telegram
TOKENS = {"T1": "bot_one", "T2": "Bot_Two", "T1NEW": "bot_one", "ADMIN": "super_admin_bot", "T3": "bot_three"}
SUPER, ALICE, MALLORY, BLOCKED = 1000, 2000, 3000, 666
lock = threading.Lock()
calls = []        # (token, method, params)
photos = []       # (token, chat_id, time, kind, file_id)
hooks = {}        # bot username -> webhook info (one per bot, like Telegram)
deleted_msgs = []
last_set = {}
not_member = {}   # chat_id -> number of sends that fail with "not a member"
fid_n = [0]


class TG(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):  # file download
        if self.path.startswith("/file/bot"):
            b = b"\xff\xd8 fake jpeg bytes"
            self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers()
            self.wfile.write(b)
        else:
            self.send_response(404); self.end_headers()

    def do_PUT(self):  # CloudFormation response URL
        cfn_responses.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()

    def do_POST(self):
        _, botpart, method = self.path.split("/", 2)
        token = botpart[3:]
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.headers.get("Content-Type", "").startswith("application/json"):
            p, upload = json.loads(body or b"{}"), False
        else:
            upload = b'name="photo"; filename=' in body
            p = {"chat_id": int(body.split(b'name="chat_id"\r\n\r\n')[1].split(b"\r\n")[0])}
        with lock:
            calls.append((token, method, p))
        if token not in TOKENS:
            return self.reply({"ok": False, "error_code": 401, "description": "Unauthorized"}, 401)
        if method == "getMe":
            return self.reply({"ok": True, "result": {"id": hash(token) % 10**6, "username": TOKENS[token],
                                                      "first_name": TOKENS[token]}})
        if method == "setWebhook":
            u = TOKENS[token]
            if time.time() - last_set.get(u, 0) < 1:  # Telegram: ~1 setWebhook per second per bot
                return self.reply({"ok": False, "error_code": 429, "description": "Too Many Requests: retry after 1",
                                   "parameters": {"retry_after": 1}}, 429)
            last_set[u] = time.time()
            hooks[TOKENS[token]] = {"url": p["url"], "allowed_updates": p["allowed_updates"], "secret": p["secret_token"]}
            return self.reply({"ok": True, "result": True})
        if method == "getWebhookInfo":
            h = hooks.get(TOKENS[token], {})
            return self.reply({"ok": True, "result": {k: v for k, v in h.items() if k != "secret"}})
        if method == "deleteWebhook":
            hooks.pop(TOKENS[token], None)
            return self.reply({"ok": True, "result": True})
        if method == "deleteMessage":
            deleted_msgs.append((token, p["message_id"]))
            return self.reply({"ok": True, "result": True})
        if method == "getFile":
            return self.reply({"ok": True, "result": {"file_id": p["file_id"], "file_path": "documents/x.jpg"}})
        if method in ("sendMessage", "setMyCommands"):
            return self.reply({"ok": True, "result": True})
        if method == "sendPhoto":
            cid = int(p["chat_id"])
            if cid in not_member and not_member[cid] > 0:  # freshly promoted channel, Telegram not caught up
                not_member[cid] -= 1
                return self.reply({"ok": False, "error_code": 403,
                                   "description": "Forbidden: bot is not a member of the channel chat"}, 403)
            if cid == BLOCKED:
                photos.append((token, cid, time.time(), "blocked", None))
                return self.reply({"ok": False, "error_code": 403, "description": "Forbidden: bot was blocked"}, 403)
            fid = p.get("photo")
            if upload:
                fid_n[0] += 1
                fid = f"UP{fid_n[0]}-{TOKENS[token]}"
            elif not str(fid).endswith(TOKENS[token]) and not str(fid).startswith("INCOMING"):
                return self.reply({"ok": False, "error_code": 400,
                                   "description": "Bad Request: wrong file identifier"}, 400)
            photos.append((token, cid, time.time(), "upload" if upload else "file_id", fid))
            return self.reply({"ok": True, "result": {"photo": [{"file_id": "small"}, {"file_id": fid}]}})
        self.reply({"ok": False, "error_code": 404, "description": "Not Found"}, 404)


cfn_responses = []
srv = ThreadingHTTPServer(("127.0.0.1", 0), TG)
threading.Thread(target=srv.serve_forever, daemon=True).start()
os.environ.update({
    "AWS_DEFAULT_REGION": "eu-central-1", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
    "BOTS_TABLE": "bots", "SUBS_TABLE": "subs", "STATE_TABLE": "state", "LEGACY_USERS_TABLE": "users",
    "SSM_PREFIX": "/pb", "API_BASE": f"http://127.0.0.1:{srv.server_port}", "PERIOD": "6", "CATCHUP_GRACE": "4",
    "LOOKAHEAD": "40", "SCHEDULE_MARGIN": "2", "SCHEDULE_GROUP": "posture-bot", "CACHE_TTL": "60",
    "SCHEDULER_ROLE_ARN": "arn:aws:iam::123456789012:role/sched",
    "BROADCAST_ARN": "arn:aws:lambda:eu-central-1:123456789012:function:posture-bot-broadcast",
    "DLQ_ARN": "arn:aws:sqs:eu-central-1:123456789012:dlq",
    "WEBHOOK_URL": "https://bots.example/", "ADMIN_WEBHOOK_URL": "https://admin.example/",
})

from moto import mock_aws  # noqa: E402


def ok(m):
    print("PASS", m)


class Ctx:
    def __init__(self, budget=(900_000,)):
        self.budget = list(budget)
        self.log_stream_name = "test"

    def get_remaining_time_in_millis(self):
        return self.budget.pop(0) if len(self.budget) > 1 else self.budget[0]


uid_n = [100]


def msg(chat_id, text=None, frm=None, **extra):
    uid_n[0] += 1
    m = {"message_id": uid_n[0], "chat": {"id": chat_id, "type": "private" if chat_id > 0 else "channel",
                                          "first_name": "x"}, "from": {"id": frm or chat_id}, **extra}
    if text is not None:
        m["text"] = text
    return {"update_id": uid_n[0], "message": m}


with mock_aws():
    import boto3
    ddb = boto3.client("dynamodb")
    for name, keys in {"bots": [("username", "S")], "subs": [("bot", "S"), ("chat_id", "N")],
                       "state": [("pk", "S"), ("sk", "S")], "users": [("chat_id", "N")]}.items():
        ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                         AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in keys],
                         KeySchema=[{"AttributeName": k, "KeyType": "HASH" if i == 0 else "RANGE"}
                                    for i, (k, _) in enumerate(keys)])
    ssmc = boto3.client("ssm")
    sched = boto3.client("scheduler")
    sched.create_schedule_group(Name="posture-bot")

    def sp(name, value):
        ssmc.put_parameter(Name=name, Value=value, Type="SecureString", Overwrite=True)

    # legacy single-bot state: token in /pb/token, 3 users, cached photo file_id
    sp("/pb/token", "T1")
    sp("/pb/super-admin", str(SUPER))
    usersT = boto3.resource("dynamodb").Table("users")
    for cid, active, typ in ((11, 1, "private"), (12, 0, "private"), (-1001, 1, "channel")):
        usersT.put_item(Item={"chat_id": cid, "active": active, "subscribed_at": 1, "type": typ, "name": "n"})

    import app
    invoked = []
    app.lambda_.invoke = lambda **kw: invoked.append(json.loads(kw["Payload"]))
    state = app.state

    def cfn(kind, grace="4", old_grace=None):
        ev = {"RequestType": kind, "ResponseURL": f"http://127.0.0.1:{srv.server_port}/cfn",
              "StackId": "s", "RequestId": f"r{len(cfn_responses)}", "LogicalResourceId": "PostDeploy",
              "ResourceProperties": {"CatchupGrace": grace}}
        if old_grace:
            ev["OldResourceProperties"] = {"CatchupGrace": old_grace}
        n = len(cfn_responses)
        app.planner(ev, Ctx())
        assert len(cfn_responses) == n + 1, "no CloudFormation response"
        return cfn_responses[-1]

    def hook(token, upd, salt=b"webhook-secret"):
        fn = app.admin_webhook if salt != b"webhook-secret" else app.webhook
        return fn({"headers": {"X-Telegram-Bot-Api-Secret-Token": app.webhook_secret(token, salt)},
                   "body": json.dumps(upd)}, Ctx())

    def ahook(upd):
        return hook("ADMIN", upd, b"admin-webhook-secret")

    def texts(token, chat):
        return [p["text"] for t, m, p in calls if t == token and m == "sendMessage" and p["chat_id"] == chat]

    def pics(token, chat, since=0):
        return [(ts, k, f) for t, c, ts, k, f in photos if t == token and c == chat and ts >= since]

    def subs_of(u):
        return {int(i["chat_id"]): int(i["active"]) for i in app.query_all(
            app.subs, KeyConditionExpression=app.Key("bot").eq(u))}

    # ---- 1. deploy: migration of the legacy bot
    r = cfn("Create")
    assert r["Status"] == "SUCCESS", r
    assert r["Data"]["Migration"] == "@bot_one: 3 subscribers migrated", r["Data"]
    assert app.param("/pb/bots/bot_one/token") == "T1" and app.param("/pb/bots/bot_one/admins") == str(SUPER)
    assert subs_of("bot_one") == {11: 1, 12: 0, -1001: 1}
    assert hooks["bot_one"]["url"] == "https://bots.example/" and hooks["bot_one"]["secret"] == app.webhook_secret("T1")
    assert r["Data"]["AdminBot"] == "admin token missing" and "super_admin_bot" not in hooks
    assert app.settings("bot_one")["status"] == "ok"
    assert cfn("Update")["Data"]["Migration"] == "@bot_one: 0 subscribers migrated"
    assert subs_of("bot_one") == {11: 1, 12: 0, -1001: 1}
    ok("deploy migrates legacy bot (token, admins=super admin, 3 subs) idempotently; webhook keeps same secret")

    # ---- 2. admin token stored later -> SSM change event registers admin webhook
    sp("/pb/admin-token", "ADMIN")
    r = app.planner({"source": "aws.ssm", "detail": {"name": "/pb/admin-token", "operation": "Create"}}, Ctx())
    assert r == {"admin": "ok"} and hooks["super_admin_bot"]["url"] == "https://admin.example/"
    assert hooks["super_admin_bot"]["secret"] == app.admin_secret("ADMIN") != app.webhook_secret("ADMIN")
    ok("admin token stored later: SSM event registers the admin bot on its own URL + secret")

    # ---- 3. admin bot: only the super admin gets answers
    n = len(calls)
    assert ahook(msg(MALLORY, "/bots"))["body"] == "ignored" and len(calls) == n
    assert hook("T1", msg(SUPER, "/bots"), b"admin-webhook-secret")["statusCode"] == 403  # wrong bot's secret
    assert app.webhook({"headers": {}, "body": "{}"}, Ctx())["statusCode"] == 403
    ok("non-super-admin ignored silently; wrong / missing secrets rejected")

    # /addbot: bad token, good token, duplicate
    ahook(msg(SUPER, "/addbot NOPE"))
    assert "rejected" in texts("ADMIN", SUPER)[-1] and deleted_msgs[-1][0] == "ADMIN"
    u = msg(SUPER, "/addbot T2")
    ahook(u)
    assert deleted_msgs[-1] == ("ADMIN", u["message"]["message_id"]), "token message not deleted"
    assert texts("ADMIN", SUPER)[-1] == "@Bot_Two added. Status: ok", texts("ADMIN", SUPER)[-1]
    assert app.param("/pb/bots/bot_two/token") == "T2" and app.param("/pb/bots/bot_two/admins") == str(SUPER)
    assert hooks["Bot_Two"]["url"] == "https://bots.example/"
    # the SSM events /addbot itself triggers arrive right away: they must not re-register or clobber status
    n_set = sum(1 for t, m, _ in calls if t == "T2" and m == "setWebhook")
    assert app.on_ssm_change({"name": "/pb/bots/bot_two/token", "operation": "Create"}) == {"bot_two": "ok"}
    assert app.on_ssm_change({"name": "/pb/bots/bot_two/admins", "operation": "Create"}) == {"bot_two": "ok"}
    assert sum(1 for t, m, _ in calls if t == "T2" and m == "setWebhook") == n_set == 1
    assert app.settings("bot_two")["status"] == "ok"
    ahook(msg(SUPER, "/addbot T2"))
    assert "already exists" in texts("ADMIN", SUPER)[-1]
    ok("/addbot: rejects bad token, adds bot (SSM token+admins, webhook) with one setWebhook despite the SSM "
       "event burst, refuses duplicate, deletes token msgs")

    # /admins add ALICE to bot_two
    ahook(msg(SUPER, "/admins bot_two add 2000"))
    assert app.parse_ids(app.param("/pb/bots/bot_two/admins")) == {SUPER, ALICE}
    ahook(msg(SUPER, "/admins @Bot_Two"))
    assert "1000, 2000" in texts("ADMIN", SUPER)[-1]
    ok("/admins add/list (names case- and @-insensitive)")

    # ---- 4. bot webhooks: per-bot subscriptions
    t0 = time.time()
    for cid in (21, 22, BLOCKED):
        hook("T2", msg(cid, "/start"))
    hook("T1", msg(21, "/start"))
    assert subs_of("bot_two") == {21: 1, 22: 1, BLOCKED: 0} and subs_of("bot_one")[21] == 1
    assert pics("T2", 21, t0) and pics("T1", 21, t0)
    assert hook("T2", msg(21, "/start"))["statusCode"] == 200
    dup = msg(22, "/stop")
    hook("T2", dup)
    assert hook("T2", dup)["body"] == "dup"
    hook("T2", msg(22, "/start"))
    ok("same chat subscribes to two bots independently; /start sends photo now; dedupe per bot")

    # ---- 5. image: admin photo / document, non-admin ignored, per bot
    p0 = len(photos)
    hook("T2", msg(MALLORY, photo=[{"file_id": "INCOMING-small"}, {"file_id": "INCOMING-mallory"}]))
    assert len(photos) == p0 and not app.settings("bot_two").get("image_file_id")
    hook("T2", msg(ALICE, photo=[{"file_id": "INCOMING-small"}, {"file_id": "INCOMING-big"}]))
    s2 = app.settings("bot_two")
    assert s2["image_file_id"] == "INCOMING-big" and int(s2["image_set_by"]) == ALICE
    assert pics("T2", ALICE)[-1][2] == "INCOMING-big"  # preview sent from the stored file_id
    hook("T1", msg(ALICE, photo=[{"file_id": "INCOMING-x"}]))  # Alice isn't bot_one's admin
    assert not app.settings("bot_one").get("image_file_id")
    hook("T2", msg(22, "/image", frm=22))
    assert not texts("T2", 22)[-1].startswith("Current")
    hook("T1", msg(SUPER, document={"file_id": "DOC1", "mime_type": "image/png", "file_name": "a.png",
                                     "file_size": 1000}))
    s1 = app.settings("bot_one")
    assert s1["image_file_id"].startswith("UP") and s1["image_file_id"].endswith("bot_one"), s1
    assert any(m == "getFile" for t, m, _ in calls if t == "T1")
    ok("admin photo -> stored file_id immediately (preview from it); image document re-uploaded; "
       "non-admins ignored; images are per bot; super admin is admin of every bot")

    # ---- 6. planner + broadcast fan-out
    plan_t = time.time()
    created = app.planner({}, Ctx())["created"]
    names = [x["Name"] for x in sched.list_schedules(GroupName="posture-bot")["Schedules"]]
    assert len(names) > 30 and all(app.slot_time(int(n[5:])) > plan_t - 6 for n in names), names
    assert all(app.slot_time(s) > plan_t for s in created)
    s = next(x for x in range(int(time.time() // 6), 10**12) if app.slot_time(x) > time.time() + 1.5)
    t = app.slot_time(s)
    invoked.clear()
    r = app.broadcast({"slot": s, "at": t - t % 60}, Ctx())
    assert r == {"bots": ["bot_one", "bot_two"]} and [i["bot"] for i in invoked] == ["bot_one", "bot_two"], (r, invoked)
    res = {i["bot"]: app.broadcast(i, Ctx()) for i in list(invoked)}
    assert res == {"bot_one": {"sent": 3, "failed": 0}, "bot_two": {"sent": 2, "failed": 0}}, res
    p2 = {c: pics("T2", c, t) for c in (21, 22)}
    assert all(v and v[0][2] == "INCOMING-big" for v in p2.values()), p2
    assert pics("T1", 11, t)[0][2] == s1["image_file_id"] and pics("T1", -1001, t)
    lag = max(v[0][0] for v in p2.values()) - t
    assert 0 <= min(v[0][0] for v in p2.values()) - t and lag < 1.0, lag
    assert app.broadcast(invoked[0], Ctx()) == {"skipped": "done"}
    ok(f"slot fans out per bot; each sends its own image at the same second (lag {lag:.2f}s); re-run sends nothing")

    # ---- 7. disable / enable
    ahook(msg(SUPER, "/disable bot_two"))
    s = next(x for x in range(int(time.time() // 6), 10**12) if app.slot_time(x) > time.time() + 0.5)
    invoked.clear()
    assert app.broadcast({"slot": s}, Ctx()) == {"bots": ["bot_one"]}
    hook("T2", msg(23, "/start"))
    assert not pics("T2", 23) and "paused" in texts("T2", 23)[-1]
    ahook(msg(SUPER, "/enable bot_two"))
    invoked.clear()
    assert app.broadcast({"slot": s}, Ctx())["bots"] == ["bot_one", "bot_two"]
    ok("/disable pauses a bot's hourly posts and immediate photo; /enable resumes")

    # ---- 8. token rotation + mismatched / invalid tokens -> status
    ahook(msg(SUPER, "/settoken T1NEW"))
    assert app.param("/pb/bots/bot_one/token") == "T1NEW" and hooks["bot_one"]["url"] == "https://bots.example/"
    assert hook("T1NEW", msg(11, "/next"))["statusCode"] == 200 and texts("T1NEW", 11)
    sp("/pb/bots/bot_one/token", "T1")  # rotated in the console: SSM event alone must re-register the secret
    assert app.on_ssm_change({"name": "/pb/bots/bot_one/token", "operation": "Update"}) == {"bot_one": "ok"}
    assert hooks["bot_one"]["secret"] == app.webhook_secret("T1"), "rotated token's secret not registered"
    assert hook("T1", msg(11, "/next"))["statusCode"] == 200 and hook("T1NEW", msg(11, "/next"))["statusCode"] == 403
    sp("/pb/bots/bot_three/token", "T2")  # console typo: bot_two's token under bot_three
    r = app.planner({"source": "aws.ssm", "detail": {"name": "/pb/bots/bot_three/token", "operation": "Create"}}, Ctx())
    assert r == {"bot_three": "token belongs to @Bot_Two, not @bot_three"}, r
    assert app.settings("bot_three")["status"].startswith("token belongs")
    sp("/pb/bots/bot_three/token", "GARBAGE")
    r = app.on_ssm_change({"name": "/pb/bots/bot_three/token", "operation": "Update"})
    assert r["bot_three"].startswith("invalid token"), r
    sp("/pb/bots/bot_three/token", "T3")
    assert app.on_ssm_change({"name": "/pb/bots/bot_three/token", "operation": "Update"}) == {"bot_three": "ok"}
    ok("/settoken and console rotation re-register the new secret (old one rejected); console mistakes show up as status in DynamoDB; fixing them syncs")

    # ---- 9. /removebot with confirmation
    ahook(msg(SUPER, "/removebot bot_three"))
    assert "Confirm" in texts("ADMIN", SUPER)[-1] and app.param("/pb/bots/bot_three/token") == "T3"
    ahook(msg(SUPER, "/removebot bot_three yes"))
    assert app.param("/pb/bots/bot_three/token") is None and not app.settings("bot_three") and "bot_three" not in hooks
    r = app.on_ssm_change({"name": "/pb/bots/bot_three/token", "operation": "Delete"})
    assert r == {"bot_three": "token missing"} and not app.settings("bot_three")  # no zombie item
    ok("/removebot asks to confirm, then deletes webhook + SSM params + settings; delete event adds no zombie")

    # ---- 10. /bots, /test, /stats, channel per bot
    ahook(msg(SUPER, "/bots"))
    listing = texts("ADMIN", SUPER)[-1]
    assert "@bot_one" in listing and "@bot_two" in listing and "custom" in listing, listing
    ahook(msg(SUPER, "/test bot_two"))
    assert pics("T2", SUPER)[-1][2] == "INCOMING-big" and texts("ADMIN", SUPER)[-1] == "Sent."
    hook("T2", msg(ALICE, "/stats"))
    assert "@bot_two: 3 active" in texts("T2", ALICE)[-1], texts("T2", ALICE)[-1]
    ch = {"id": -2002, "type": "channel", "title": "C", "username": "chan"}
    hook("T2", {"update_id": 9001, "my_chat_member": {"chat": ch, "new_chat_member": {
        "status": "administrator", "can_post_messages": True}}})
    assert subs_of("bot_two")[-2002] == 1 and pics("T2", -2002) and -2002 not in subs_of("bot_one")
    not_member[-2003] = 2  # first two sends right after promotion fail
    ch3 = {"id": -2003, "type": "channel", "title": "C3"}
    hook("T2", {"update_id": 9002, "my_chat_member": {"chat": ch3, "new_chat_member": {
        "status": "administrator", "can_post_messages": True}}})
    assert subs_of("bot_two")[-2003] == 1 and len(pics("T2", -2003)) == 1, (subs_of("bot_two"), pics("T2", -2003))
    not_member[-2003] = 1  # a send error during a broadcast never deactivates a channel
    assert app.send_with_retry(app.get_bot("bot_two"), -2003) is False and subs_of("bot_two")[-2003] == 1
    hook("T2", {"update_id": 9003, "my_chat_member": {"chat": ch3, "new_chat_member": {"status": "left"}}})
    assert subs_of("bot_two")[-2003] == 0
    ok("/bots lists status + image; /test sends via that bot; /stats for bot admins; channels subscribe per bot; "
       "first channel post retried while Telegram reports 'not a member'; only membership updates deactivate channels")

    # ---- 11. hourly planner keeps webhooks; stale secret (403) re-registered
    hooks.pop("Bot_Two")
    r = app.planner({}, Ctx())
    assert r["bots"] == {"bot_one": "ok", "bot_two": "ok"} and r["admin"] == "ok" and hooks["Bot_Two"]
    ok("hourly planner re-registers a lost webhook")

    # ---- 12. delete stack
    r = cfn("Delete")
    assert r["Status"] == "SUCCESS" and not hooks and not sched.list_schedules(GroupName="posture-bot")["Schedules"]
    app.scheduler.list_schedules = None
    assert cfn("Create")["Status"] == "FAILED"
    ok("stack delete removes schedules and every webhook; unexpected error reported as FAILED")

    leaked = [p for t, m, p in calls if "T1" in json.dumps(p) or "T2" in json.dumps(p)]
    assert not leaked, leaked
    print("ALL CHECKS PASSED")

"""Serverless posture bot: Lambda + EventBridge Scheduler + DynamoDB.

Three handlers share this module:

  webhook    Telegram webhook (Lambda function URL). /start subscribes and sends
             the photo right away, /stop unsubscribes, /next shows the next send.
  planner    Runs hourly (EventBridge Scheduler, rate(1 hour)). Makes sure a
             one-time EventBridge schedule exists for each of the next LOOKAHEAD
             hourly slots. A slot too close to schedule at its own minute (only
             right after first deploy) is scheduled SCHEDULE_MARGIN s from now
             instead. Past slots are never re-sent.
             Every run also makes sure Telegram's webhook points at WEBHOOK_URL
             (so storing/rotating the token needs no redeploy), and the planner
             doubles as a CloudFormation custom resource: `sam deploy` alone
             schedules slots and registers the webhook; stack delete removes it.
  broadcast  Invoked by a one-time schedule at the minute of the slot; sleeps to
             the exact random second, then sends the photo to every active user.

Outage tolerance:
  * Slots are deterministic: HMAC(seed, slot) mod PERIOD, seed kept in DynamoDB.
  * Schedules are created LOOKAHEAD hours ahead, so the planner can be down for a
    day without missing anything; AWS fires them on its own, retrying for up to
    CATCHUP_GRACE seconds and dead-lettering to SQS after that.
  * No late spam: a slot that can't *start* within CATCHUP_GRACE of its time is
    dropped, never sent late; the next hour goes out as usual.
  * Per-(slot, chat) delivery log + a per-slot lease make broadcast idempotent
    under Scheduler retries and Lambda async retries.
  * A broadcast that runs low on Lambda time re-invokes itself and resumes.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import math
import os
import secrets
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

E = os.environ
USERS_TABLE = E["USERS_TABLE"]
STATE_TABLE = E["STATE_TABLE"]
TOKEN_PARAM = E.get("TOKEN_PARAM", "/posture-bot/token")
API_BASE = E.get("API_BASE", "https://api.telegram.org").rstrip("/")
PERIOD = int(E.get("PERIOD", "3600"))
GRACE = int(E.get("CATCHUP_GRACE", "120"))  # max lateness at which a slot still starts
SCHEDULE_MARGIN = int(E.get("SCHEDULE_MARGIN", "90"))  # never create a schedule closer than this
SEND_RATE = float(E.get("SEND_RATE", "25"))
CAPTION = E.get("CAPTION", "")
LOOKAHEAD = int(E.get("LOOKAHEAD", "24"))
GROUP = E.get("SCHEDULE_GROUP", "posture-bot")
SCHEDULER_ROLE_ARN = E.get("SCHEDULER_ROLE_ARN", "")
BROADCAST_ARN = E.get("BROADCAST_ARN", "")
DLQ_ARN = E.get("DLQ_ARN", "")
DISPLAY_TZ = ZoneInfo(E.get("DISPLAY_TZ", "Europe/Berlin"))
# Channels the bot may post to once made admin: comma-separated @usernames and/or
# numeric ids (-100...). Empty = any channel that grants "Post messages".
ALLOWED_CHANNELS = {c.strip().lower() for c in E.get("ALLOWED_CHANNELS", "").split(",") if c.strip()}
PHOTO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "photo.jpg")
DAY = 86400
WEBHOOK_URL = E.get("WEBHOOK_URL", "")  # set on the planner only (avoids a template cycle)
ALLOWED_UPDATES = ["message", "my_chat_member"]

ddb = boto3.resource("dynamodb")
users = ddb.Table(USERS_TABLE)
state = ddb.Table(STATE_TABLE)
ssm = boto3.client("ssm")
scheduler = boto3.client("scheduler")
lambda_ = boto3.client("lambda")


# ---------------------------------------------------------------- Telegram
class ApiError(Exception):
    def __init__(self, code: int, desc: str, retry_after: int | None = None):
        super().__init__(f"{code}: {desc}")
        self.code, self.desc, self.retry_after = code, desc, retry_after

    @property
    def gone(self) -> bool:  # user blocked the bot / chat deleted
        return self.code == 403 or "chat not found" in self.desc.lower()


_token: tuple[str, float] | None = None


def token() -> str:
    """Cached for 5 min so a rotated/fixed token is picked up by warm containers."""
    global _token
    if _token is None or time.time() - _token[1] > 300:
        _token = (ssm.get_parameter(Name=TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"].strip(),
                  time.time())
    return _token[0]


def webhook_secret() -> str:
    return hmac.new(token().encode(), b"webhook-secret", hashlib.sha256).hexdigest()


def _multipart(fields: dict, files: dict) -> tuple[bytes, str]:
    boundary = uuid.uuid4().hex
    body = b""
    for k, v in fields.items():
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode()
    for k, (fname, data, ctype) in files.items():
        body += (f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"; filename="{fname}"\r\n'
                 f"Content-Type: {ctype}\r\n\r\n").encode() + data + b"\r\n"
    return body + f"--{boundary}--\r\n".encode(), f"multipart/form-data; boundary={boundary}"


def api(method: str, params: dict | None = None, files: dict | None = None, timeout: float = 30):
    params = {k: v for k, v in (params or {}).items() if v is not None}
    if files:
        body, ctype = _multipart({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                  for k, v in params.items()}, files)
    else:
        body, ctype = json.dumps(params).encode(), "application/json"
    req = urllib.request.Request(f"{API_BASE}/bot{token()}/{method}", data=body,
                                 headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.load(r)
    except urllib.error.HTTPError as e:
        try:
            data = json.load(e)
        except Exception:
            raise ApiError(e.code, str(e)) from None
    if not data.get("ok"):
        raise ApiError(data.get("error_code", 0), data.get("description", ""),
                       (data.get("parameters") or {}).get("retry_after"))
    return data["result"]


_photo: tuple[bytes, str] | None = None


def send_photo(chat_id: int) -> None:
    """Upload once, then reuse Telegram's file_id (re-upload if photo.jpg changes)."""
    global _photo
    if _photo is None:
        data = open(PHOTO_PATH, "rb").read()
        _photo = (data, hashlib.sha256(data).hexdigest())
    data, sha = _photo
    meta = state.get_item(Key={"pk": "meta", "sk": "photo"}).get("Item") or {}
    if meta.get("sha") == sha:
        try:
            api("sendPhoto", {"chat_id": chat_id, "photo": meta["file_id"], "caption": CAPTION or None})
            return
        except ApiError as e:
            if not (e.code == 400 and "file" in e.desc.lower()):
                raise
            log.warning("cached file_id rejected (%s); re-uploading", e.desc)
    res = api("sendPhoto", {"chat_id": chat_id, "caption": CAPTION or None},
              files={"photo": ("photo.jpg", data, "image/jpeg")}, timeout=60)
    state.put_item(Item={"pk": "meta", "sk": "photo", "sha": sha, "file_id": res["photo"][-1]["file_id"]})


def send_with_retry(chat_id: int) -> bool:
    """True if delivered. Deactivates users who blocked the bot."""
    for attempt in range(4):
        try:
            send_photo(chat_id)
            return True
        except ApiError as e:
            if e.retry_after:
                log.warning("rate limited; sleeping %ss", e.retry_after)
                time.sleep(e.retry_after + 1)
                continue
            if e.gone:
                log.info("deactivating %s: %s", chat_id, e.desc)
                set_active(chat_id, False)
            else:
                log.error("send to %s failed: %s", chat_id, e)
            return False
        except (urllib.error.URLError, OSError) as e:
            log.error("network error sending to %s: %s", chat_id, e)
            time.sleep(min(2 ** attempt, 10))
    return False


def reply(chat_id: int, text: str) -> None:
    try:
        api("sendMessage", {"chat_id": chat_id, "text": text})
    except Exception as e:
        log.warning("reply to %s failed: %s", chat_id, e)


# ---------------------------------------------------------------- schedule
_seed: bytes | None = None


def seed() -> bytes:
    global _seed
    if _seed is None:
        try:
            state.put_item(Item={"pk": "meta", "sk": "seed", "v": secrets.token_hex(32)},
                           ConditionExpression="attribute_not_exists(pk)")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        item = state.get_item(Key={"pk": "meta", "sk": "seed"}, ConsistentRead=True)["Item"]
        _seed = bytes.fromhex(item["v"])
    return _seed


def slot_time(slot: int) -> int:
    h = hmac.new(seed(), str(slot).encode(), hashlib.sha256).digest()
    return slot * PERIOD + int.from_bytes(h[:8], "big") % PERIOD


def next_slot_time(now: float) -> int:
    s = int(now // PERIOD)
    while slot_time(s) <= now:
        s += 1
    return slot_time(s)


def fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, DISPLAY_TZ).strftime("%H:%M:%S %Z, %d %b")


def is_done(slot: int) -> bool:
    return "Item" in state.get_item(Key={"pk": "done", "sk": str(slot)})


# ---------------------------------------------------------------- users
def set_active(chat_id: int, active: bool) -> None:
    users.update_item(Key={"chat_id": chat_id}, UpdateExpression="SET active = :a",
                      ExpressionAttributeValues={":a": int(active)})


def subscribe(chat: dict) -> None:
    users.put_item(Item={"chat_id": chat["id"], "active": 1, "subscribed_at": math.ceil(time.time()),
                         "type": chat.get("type", ""),
                         "name": chat.get("title") or chat.get("username") or chat.get("first_name") or ""})


def channel_allowed(chat: dict) -> bool:
    if not ALLOWED_CHANNELS:
        return True
    ids = {str(chat["id"])}
    if chat.get("username"):
        ids.add("@" + chat["username"].lower())
    return bool(ids & ALLOWED_CHANNELS)


def handle_channel_membership(chat: dict, member: dict) -> None:
    """Channels subscribe by making the bot an admin with "Post messages"."""
    cid = chat["id"]
    can_post = member["status"] == "creator" or (
        member["status"] == "administrator" and member.get("can_post_messages", False))
    if not can_post:
        set_active(cid, False)
        log.info("channel %s (%s): can't post, deactivated", cid, chat.get("title"))
        return
    if not channel_allowed(chat):
        log.warning("channel %s (@%s) not in ALLOWED_CHANNELS; ignoring", cid, chat.get("username"))
        set_active(cid, False)
        return
    prev = users.get_item(Key={"chat_id": cid}).get("Item")
    if prev and int(prev.get("active", 0)):
        return  # e.g. other admin rights changed; already subscribed
    subscribe(chat)
    log.info("channel %s (%s) subscribed", cid, chat.get("title"))
    send_with_retry(cid)  # first post right away, like /start


def handle_update(upd: dict) -> None:
    mcm = upd.get("my_chat_member")
    if mcm:
        member = mcm["new_chat_member"]
        if member["status"] in ("kicked", "left"):
            set_active(mcm["chat"]["id"], False)
        elif mcm["chat"]["type"] == "channel":
            handle_channel_membership(mcm["chat"], member)
        return
    msg = upd.get("message")  # channel posts are ignored: channels subscribe via admin rights
    if not msg or "text" not in msg:
        return
    cmd = msg["text"].split()[0].split("@")[0].lower()
    chat = msg["chat"]
    cid = chat["id"]
    if cmd == "/start":
        subscribe(chat)
        reply(cid, "Subscribed: one reminder every hour at a random second.\n"
                   f"Next one: {fmt(next_slot_time(time.time()))}\n/stop to unsubscribe.")
        send_with_retry(cid)  # first one right away; not counted as an hourly slot
    elif cmd == "/stop":
        set_active(cid, False)
        reply(cid, "Unsubscribed. /start to resume.")
    elif cmd == "/next":
        reply(cid, f"Next reminder: {fmt(next_slot_time(time.time()))}")


# ---------------------------------------------------------------- handlers
def webhook(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    try:
        expected = webhook_secret()
    except ssm.exceptions.ParameterNotFound:
        log.error("token parameter %s missing", TOKEN_PARAM)
        return {"statusCode": 503, "body": "not configured"}
    if not hmac.compare_digest(headers.get("x-telegram-bot-api-secret-token", ""), expected):
        return {"statusCode": 403, "body": "forbidden"}
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()
    upd = json.loads(body)
    try:  # Telegram re-delivers on timeouts; process each update once
        state.put_item(Item={"pk": "upd", "sk": str(upd.get("update_id")), "ttl": int(time.time()) + DAY},
                       ConditionExpression="attribute_not_exists(pk)")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return {"statusCode": 200, "body": "dup"}
        raise
    try:
        handle_update(upd)
    except Exception:
        log.exception("update %s failed", upd.get("update_id"))
    return {"statusCode": 200, "body": "ok"}


def _invoke_continuation(slot: int) -> None:
    lambda_.invoke(FunctionName=BROADCAST_ARN, InvocationType="Event",
                   Payload=json.dumps({"slot": slot, "continuation": True}).encode())


def ensure_webhook(force: bool = False) -> str:
    """Point Telegram's webhook at WEBHOOK_URL. Never raises: returns a status."""
    if not WEBHOOK_URL:
        return "no WEBHOOK_URL"
    try:
        token()
    except ssm.exceptions.ParameterNotFound:
        log.warning("token %s not stored yet; webhook not registered", TOKEN_PARAM)
        return "token missing"
    try:
        if not force:
            info = api("getWebhookInfo")
            if (info.get("url") == WEBHOOK_URL
                    and sorted(info.get("allowed_updates") or []) == sorted(ALLOWED_UPDATES)
                    and "403" not in (info.get("last_error_message") or "")):  # 403 = stale secret (token rotated)
                return "ok"
        api("setWebhook", {"url": WEBHOOK_URL, "secret_token": webhook_secret(),
                           "allowed_updates": ALLOWED_UPDATES, "drop_pending_updates": False})
        log.info("webhook registered: %s", WEBHOOK_URL)
        return "set"
    except Exception as e:  # bad token etc. must not break scheduling or the stack
        log.error("webhook registration failed: %s", e)
        return f"error: {e}"


def delete_slot_schedules() -> int:
    n = 0
    for page in scheduler.get_paginator("list_schedules").paginate(GroupName=GROUP, NamePrefix="slot-"):
        for sch in page["Schedules"]:
            scheduler.delete_schedule(GroupName=GROUP, Name=sch["Name"])
            n += 1
    return n


def _cfn_respond(event, context, status: str, reason: str = "", data: dict | None = None) -> None:
    body = json.dumps({
        "Status": status, "PhysicalResourceId": "posture-bot-setup",
        "Reason": (reason or "see CloudWatch log stream " + getattr(context, "log_stream_name", ""))[:900],
        "StackId": event["StackId"], "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"], "Data": data or {}}).encode()
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=30).read()


def _cfn(event, context):
    """Custom::PostDeploy: runs on create/update of the stack, cleans up on delete.
    Always answers CloudFormation, or the stack would hang for an hour."""
    try:
        kind = event["RequestType"]
        if kind == "Delete":
            removed = delete_slot_schedules()
            hook = "skipped"
            try:
                api("deleteWebhook")
                hook = "deleted"
            except Exception as e:
                hook = f"not deleted: {e}"
            data = {"Schedules": f"{removed} deleted", "Webhook": hook}
        else:
            new, old = event.get("ResourceProperties", {}), event.get("OldResourceProperties", {})
            removed = 0
            if kind == "Update" and str(new.get("CatchupGrace")) != str(old.get("CatchupGrace")):
                removed = delete_slot_schedules()  # recreate with the new retry window
            created = plan(time.time())
            data = {"Schedules": f"{removed} recreated, {len(created)} created", "Webhook": ensure_webhook(force=True)}
        log.info("custom resource %s: %s", kind, data)
        _cfn_respond(event, context, "SUCCESS", f"{data}", data)
    except Exception as e:
        log.exception("custom resource failed")
        _cfn_respond(event, context, "FAILED", f"{type(e).__name__}: {e}")


def planner(event, context):
    event = event or {}
    if "RequestType" in event and "ResponseURL" in event:
        return _cfn(event, context)
    created = plan(time.time())
    return {"created": created, "webhook": ensure_webhook()}


def plan(now: float) -> list[int]:
    """Ensure a one-time schedule exists for each of the next LOOKAHEAD slots."""
    existing = set()
    for page in scheduler.get_paginator("list_schedules").paginate(GroupName=GROUP, NamePrefix="slot-"):
        existing |= {s["Name"] for s in page["Schedules"]}
    created = []
    for s in range(int(now // PERIOD), int(now // PERIOD) + LOOKAHEAD + 1):
        name, t = f"slot-{s}", slot_time(s)
        if name in existing or t <= now:  # past slots are never re-sent
            continue
        # fire at the slot's minute (Scheduler is minute-granular; broadcast sleeps the
        # rest). If that's too close (first deploy), push it SCHEDULE_MARGIN s out;
        # broadcast then starts late by at most ~SCHEDULE_MARGIN, judged against "at".
        fire = max(t - t % 60, math.ceil(now) + SCHEDULE_MARGIN)
        try:
            scheduler.create_schedule(
                Name=name, GroupName=GROUP,
                ScheduleExpression=f"at({datetime.fromtimestamp(fire, timezone.utc):%Y-%m-%dT%H:%M:%S})",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                ActionAfterCompletion="DELETE",
                Target={"Arn": BROADCAST_ARN, "RoleArn": SCHEDULER_ROLE_ARN,
                        "Input": json.dumps({"slot": s, "at": fire}),
                        "RetryPolicy": {"MaximumEventAgeInSeconds": max(60, min(GRACE, DAY)),
                                        "MaximumRetryAttempts": 185},
                        **({"DeadLetterConfig": {"Arn": DLQ_ARN}} if DLQ_ARN else {})},
                Description=f"posture-bot slot at {datetime.fromtimestamp(t, timezone.utc):%Y-%m-%dT%H:%M:%SZ}")
            created.append(s)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConflictException":
                raise
    log.info("planner: created %s, existing %d", created, len(existing))
    return created


def broadcast(event, context):
    slot = int(event["slot"])
    t = slot_time(slot)
    due = max(t, int(event.get("at", t)))  # "at" > t only for slots pushed out by the planner
    late = time.time() - due
    if late > GRACE and not event.get("continuation"):
        log.warning("slot %d would start %ds late (> grace %ds); dropping", slot, late, GRACE)
        return {"skipped": "stale"}
    if is_done(slot):
        return {"skipped": "done"}
    wait = t - time.time()
    if wait > context.get_remaining_time_in_millis() / 1000 - 120:
        raise RuntimeError(f"invoked {wait:.0f}s before slot {slot}; refusing")
    if wait > 0:
        time.sleep(wait)

    lease_until = int(time.time() + context.get_remaining_time_in_millis() / 1000 + 30)
    try:
        state.put_item(Item={"pk": "lock", "sk": str(slot), "until": lease_until, "ttl": lease_until + DAY},
                       ConditionExpression="attribute_not_exists(pk) OR #u < :now",
                       ExpressionAttributeNames={"#u": "until"},
                       ExpressionAttributeValues={":now": int(time.time())})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            log.info("slot %d: another run holds the lease", slot)
            return {"skipped": "locked"}
        raise
    try:
        result = _broadcast_locked(slot, t, context)
    finally:
        state.delete_item(Key={"pk": "lock", "sk": str(slot)})
    if result.get("continued"):
        _invoke_continuation(slot)  # only after releasing the lease, so the continuation can take it
    return result


def _broadcast_locked(slot: int, t: int, context):
    delivered = set()
    kw = {"KeyConditionExpression": Key("pk").eq(f"sent#{slot}"), "ProjectionExpression": "sk",
          "ConsistentRead": True}
    while True:
        page = state.query(**kw)
        delivered |= {int(i["sk"]) for i in page["Items"]}
        if "LastEvaluatedKey" not in page:
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    pending = []
    kw = {"FilterExpression": Attr("active").eq(1) & Attr("subscribed_at").lte(t),
          "ProjectionExpression": "chat_id"}
    while True:
        page = users.scan(**kw)
        pending += [int(i["chat_id"]) for i in page["Items"] if int(i["chat_id"]) not in delivered]
        if "LastEvaluatedKey" not in page:
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    pending.sort()
    log.info("slot %d (%s): %d to send, %d already delivered", slot, fmt(t), len(pending), len(delivered))

    sent = failed = 0
    for chat_id in pending:  # once started, a round always finishes (grace only gates the start)
        if context.get_remaining_time_in_millis() < 30_000:
            log.info("slot %d: low on time after %d sends; continuing in a new invocation", slot, sent)
            return {"sent": sent, "failed": failed, "continued": True}
        if send_with_retry(chat_id):
            state.put_item(Item={"pk": f"sent#{slot}", "sk": str(chat_id), "ttl": int(time.time()) + 2 * DAY})
            sent += 1
        else:
            failed += 1
        time.sleep(1 / SEND_RATE)
    state.put_item(Item={"pk": "done", "sk": str(slot), "ttl": int(time.time()) + 2 * DAY})
    log.info("slot %d: done, sent %d, failed %d", slot, sent, failed)
    return {"sent": sent, "failed": failed}

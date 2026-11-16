"""Serverless posture bot (multi-bot): Lambda + EventBridge Scheduler + DynamoDB + SSM.

Any number of Telegram bots do the same thing: send their image to every
subscriber once an hour at a shared random second.

Where things live
  SSM  {P}/bots/<username>/token   SecureString  BotFather token (source of truth
                                                 for which bots exist)
       {P}/bots/<username>/admins  SecureString  comma-separated Telegram user ids
       {P}/admin-token             SecureString  token of the super-admin bot
       {P}/super-admin             SecureString  Telegram user id of the super admin
  DynamoDB bots   per-bot settings + status (enabled, caption, allowed_channels,
                  image_file_id, status, checked_at); never a token
           subs   (bot, chat_id) subscribers: private chats and channels
           state  seed, per-(bot, slot) lock / sent log / done marker, update dedupe

Handlers
  webhook        Shared webhook for all bots (one Function URL). The bot is
                 identified by its secret_token header. /start /stop /next /id;
                 admins also /image /stats and set the image by sending a photo.
  admin_webhook  Super-admin bot (own Function URL): /addbot /settoken /bots
                 /enable /disable /removebot /admins /test. Only the super admin.
  planner        Hourly: keeps 24 h of one-time slot schedules and re-syncs every
                 bot (getMe, webhook, commands). Also runs on SSM parameter changes
                 (EventBridge) and as the CloudFormation custom resource, so
                 `sam deploy` alone sets everything up.
  broadcast      Fired by a slot schedule: fans out one async invocation per
                 enabled bot; each sleeps to the exact second and sends.

Delivery rules: a slot that can't start within CATCHUP_GRACE of its time is
dropped, never sent late; a started round always finishes; per-(bot, slot)
lease + delivery log make retries resend only to chats without a recorded send.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import math
import os
import re
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
P = E.get("SSM_PREFIX", "/posture-bot").rstrip("/")
BOTS_PATH = f"{P}/bots"
ADMIN_TOKEN_PARAM = f"{P}/admin-token"
SUPER_ADMIN_PARAM = f"{P}/super-admin"
LEGACY_TOKEN_PARAM = f"{P}/token"
BOTS_TABLE, SUBS_TABLE, STATE_TABLE = E["BOTS_TABLE"], E["SUBS_TABLE"], E["STATE_TABLE"]
LEGACY_USERS_TABLE = E.get("LEGACY_USERS_TABLE", "")
API_BASE = E.get("API_BASE", "https://api.telegram.org").rstrip("/")
PERIOD = int(E.get("PERIOD", "3600"))
GRACE = int(E.get("CATCHUP_GRACE", "120"))  # max lateness at which a slot still starts
SCHEDULE_MARGIN = int(E.get("SCHEDULE_MARGIN", "90"))  # never create a schedule closer than this
SEND_RATE = float(E.get("SEND_RATE", "25"))
LOOKAHEAD = int(E.get("LOOKAHEAD", "24"))
GROUP = E.get("SCHEDULE_GROUP", "posture-bot")
SCHEDULER_ROLE_ARN = E.get("SCHEDULER_ROLE_ARN", "")
BROADCAST_ARN = E.get("BROADCAST_ARN", "")
DLQ_ARN = E.get("DLQ_ARN", "")
DISPLAY_TZ = ZoneInfo(E.get("DISPLAY_TZ", "Europe/Berlin"))
WEBHOOK_URL = E.get("WEBHOOK_URL", "")  # shared bots webhook
ADMIN_WEBHOOK_URL = E.get("ADMIN_WEBHOOK_URL", "")  # super-admin bot webhook
CACHE_TTL = int(E.get("CACHE_TTL", "60"))
PHOTO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "photo.jpg")
DAY = 86400
ALLOWED_UPDATES = ["message", "my_chat_member"]
USER_COMMANDS = [{"command": "start", "description": "Subscribe to hourly reminders"},
                 {"command": "stop", "description": "Unsubscribe"},
                 {"command": "next", "description": "When is the next reminder"}]
BOT_ADMIN_COMMANDS = USER_COMMANDS + [
    {"command": "image", "description": "Show the current image (send a photo to replace it)"},
    {"command": "stats", "description": "Subscriber counts"},
    {"command": "id", "description": "Your Telegram user id"}]
SUPER_COMMANDS = [
    {"command": "bots", "description": "List bots"},
    {"command": "addbot", "description": "/addbot <token>"},
    {"command": "settoken", "description": "/settoken <token>"},
    {"command": "enable", "description": "/enable <bot>"},
    {"command": "disable", "description": "/disable <bot>"},
    {"command": "removebot", "description": "/removebot <bot>"},
    {"command": "admins", "description": "/admins <bot> [add|del <user_id>]"},
    {"command": "test", "description": "/test <bot>: send its image to you"}]

ddb = boto3.resource("dynamodb")
bots = ddb.Table(BOTS_TABLE)
subs = ddb.Table(SUBS_TABLE)
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

    @property
    def bad_file(self) -> bool:
        return self.code == 400 and "file" in self.desc.lower()


def _multipart(fields: dict, files: dict) -> tuple[bytes, str]:
    boundary = uuid.uuid4().hex
    body = b""
    for k, v in fields.items():
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode()
    for k, (fname, data, ctype) in files.items():
        body += (f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"; filename="{fname}"\r\n'
                 f"Content-Type: {ctype}\r\n\r\n").encode() + data + b"\r\n"
    return body + f"--{boundary}--\r\n".encode(), f"multipart/form-data; boundary={boundary}"


def api(token: str, method: str, params: dict | None = None, files: dict | None = None, timeout: float = 30):
    try:
        return _api(token, method, params, files, timeout)
    except ApiError as e:  # setWebhook etc. are rate limited per bot (~1/s): wait it out once
        if method == "sendPhoto" or not e.retry_after or e.retry_after > 10:
            raise
        time.sleep(e.retry_after + 0.5)
        return _api(token, method, params, files, timeout)


def _api(token: str, method: str, params: dict | None, files: dict | None, timeout: float):
    params = {k: v for k, v in (params or {}).items() if v is not None}
    if files:
        body, ctype = _multipart({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                  for k, v in params.items()}, files)
    else:
        body, ctype = json.dumps(params).encode(), "application/json"
    req = urllib.request.Request(f"{API_BASE}/bot{token}/{method}", data=body, headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.load(r)
    except urllib.error.HTTPError as e:
        try:
            data = json.load(e)
        except Exception:
            raise ApiError(e.code, str(e.reason)) from None
    if not data.get("ok"):
        raise ApiError(data.get("error_code", 0), data.get("description", ""),
                       (data.get("parameters") or {}).get("retry_after"))
    return data["result"]


def download(token: str, file_id: str) -> bytes:
    path = api(token, "getFile", {"file_id": file_id})["file_path"]
    with urllib.request.urlopen(f"{API_BASE}/file/bot{token}/{path}", timeout=60) as r:
        return r.read()


def webhook_secret(token: str, salt: bytes = b"webhook-secret") -> str:
    return hmac.new(token.encode(), salt, hashlib.sha256).hexdigest()


def admin_secret(token: str) -> str:
    return webhook_secret(token, b"admin-webhook-secret")


def say(token: str, chat_id: int, text: str) -> None:
    try:
        api(token, "sendMessage", {"chat_id": chat_id, "text": text})
    except Exception as e:
        log.warning("message to %s failed: %s", chat_id, e)


# ---------------------------------------------------------------- SSM + caches
_cache: dict[str, tuple[float, object]] = {}


def cached(key: str, fn, force: bool = False):
    hit = _cache.get(key)
    if force or hit is None or time.time() - hit[0] > CACHE_TTL:
        hit = (time.time(), fn())
        _cache[key] = hit
    return hit[1]


def param(name: str) -> str | None:
    try:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"].strip()
    except ssm.exceptions.ParameterNotFound:
        return None


def put_param(name: str, value: str, overwrite: bool = True) -> None:
    ssm.put_parameter(Name=name, Value=value, Type="SecureString", Overwrite=overwrite)


def parse_ids(s: str | None) -> set[int]:
    return {int(x) for x in re.split(r"[,\s]+", s or "") if re.fullmatch(r"-?\d+", x)}


def super_admin(force: bool = False) -> int | None:
    ids = parse_ids(cached("super", lambda: param(SUPER_ADMIN_PARAM), force))
    return next(iter(ids)) if len(ids) == 1 else None


def admin_token(force: bool = False) -> str | None:
    return cached("admin-token", lambda: param(ADMIN_TOKEN_PARAM), force)


def _fetch_bots() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for page in ssm.get_paginator("get_parameters_by_path").paginate(
            Path=BOTS_PATH, Recursive=True, WithDecryption=True):
        for p in page["Parameters"]:
            parts = p["Name"][len(BOTS_PATH) + 1:].split("/")
            if len(parts) == 2:
                out.setdefault(parts[0].lower(), {})[parts[1]] = p["Value"].strip()
    return {u: {"token": v["token"], "admins": parse_ids(v.get("admins"))} for u, v in out.items() if v.get("token")}


def bot_secrets(force: bool = False) -> dict[str, dict]:
    """username -> {"token", "admins"} from SSM, cached CACHE_TTL s."""
    return cached("bots", _fetch_bots, force)


def norm(u: str) -> str:
    return u.strip().lstrip("@").lower()


# ---------------------------------------------------------------- bots (settings)
def scan_all(table, **kw) -> list[dict]:
    items = []
    while True:
        page = table.scan(**kw)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def query_all(table, **kw) -> list[dict]:
    items = []
    while True:
        page = table.query(**kw)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def settings(u: str) -> dict:
    return bots.get_item(Key={"username": u}, ConsistentRead=True).get("Item") or {}


def update_settings(u: str, create: bool = True, **fields) -> None:
    sets, names, values = [], {}, {}
    if "enabled" not in fields:  # new bots start enabled
        sets.append("enabled = if_not_exists(enabled, :true)")
        values[":true"] = True
    for i, (k, v) in enumerate(fields.items()):
        names[f"#f{i}"], values[f":v{i}"] = k, v
        sets.append(f"#f{i} = :v{i}")
    kw = {"Key": {"username": u}, "UpdateExpression": "SET " + ", ".join(sets),
          "ExpressionAttributeValues": values}
    if names:
        kw["ExpressionAttributeNames"] = names
    if not create:
        kw["ConditionExpression"] = "attribute_exists(username)"
    try:
        bots.update_item(**kw)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


class Bot:
    def __init__(self, username: str, token: str, admins: set[int]):
        self.username, self.token, self.admins = username, token, admins
        self._s: dict | None = None

    @property
    def s(self) -> dict:
        if self._s is None:
            self._s = settings(self.username)
        return self._s

    @property
    def enabled(self) -> bool:
        return self.s.get("enabled", True) is not False

    def is_admin(self, uid) -> bool:
        return uid is not None and (uid in self.admins or uid == super_admin())


def get_bot(u: str, force: bool = False) -> Bot | None:
    b = bot_secrets(force).get(norm(u))
    return Bot(norm(u), b["token"], b["admins"]) if b else None


# ---------------------------------------------------------------- sending
_default: tuple[bytes, str] | None = None


def default_photo() -> tuple[bytes, str]:
    global _default
    if _default is None:
        data = open(PHOTO_PATH, "rb").read()
        _default = (data, hashlib.sha256(data).hexdigest())
    return _default


def send_photo(bot: Bot, chat_id: int, caption: str | None = None) -> None:
    """Bot's own image by file_id; else the bundled photo, uploaded once per bot."""
    s = bot.s
    cap = caption if caption is not None else (s.get("caption") or None)
    if s.get("image_file_id"):
        try:
            api(bot.token, "sendPhoto", {"chat_id": chat_id, "photo": s["image_file_id"], "caption": cap})
            return
        except ApiError as e:
            if not e.bad_file:
                raise
            log.error("@%s: image file_id rejected (%s); using default photo", bot.username, e.desc)
    data, sha = default_photo()
    if s.get("default_sha") == sha and s.get("default_file_id"):
        try:
            api(bot.token, "sendPhoto", {"chat_id": chat_id, "photo": s["default_file_id"], "caption": cap})
            return
        except ApiError as e:
            if not e.bad_file:
                raise
    res = api(bot.token, "sendPhoto", {"chat_id": chat_id, "caption": cap},
              files={"photo": ("photo.jpg", data, "image/jpeg")}, timeout=60)
    fid = res["photo"][-1]["file_id"]
    update_settings(bot.username, default_file_id=fid, default_sha=sha)
    s.update(default_file_id=fid, default_sha=sha)


def send_with_retry(bot: Bot, chat_id: int) -> bool:
    """True if delivered. Deactivates chats that blocked the bot."""
    for attempt in range(4):
        try:
            send_photo(bot, chat_id)
            return True
        except ApiError as e:
            if e.retry_after:
                log.warning("@%s rate limited; sleeping %ss", bot.username, e.retry_after)
                time.sleep(e.retry_after + 1)
                continue
            if e.gone:
                log.info("@%s: deactivating %s: %s", bot.username, chat_id, e.desc)
                set_active(bot, chat_id, False)
            else:
                log.error("@%s: send to %s failed: %s", bot.username, chat_id, e)
            return False
        except (urllib.error.URLError, OSError) as e:
            log.error("@%s: network error sending to %s: %s", bot.username, chat_id, e)
            time.sleep(min(2 ** attempt, 10))
    return False


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
        _seed = bytes.fromhex(state.get_item(Key={"pk": "meta", "sk": "seed"}, ConsistentRead=True)["Item"]["v"])
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


# ---------------------------------------------------------------- subscribers
def set_active(bot: Bot, chat_id: int, active: bool) -> None:
    subs.update_item(Key={"bot": bot.username, "chat_id": chat_id}, UpdateExpression="SET active = :a",
                     ExpressionAttributeValues={":a": int(active)})


def subscribe(bot: Bot, chat: dict) -> None:
    subs.put_item(Item={"bot": bot.username, "chat_id": chat["id"], "active": 1,
                        "subscribed_at": math.ceil(time.time()), "type": chat.get("type", ""),
                        "name": chat.get("title") or chat.get("username") or chat.get("first_name") or ""})


def channel_allowed(bot: Bot, chat: dict) -> bool:
    allowed = {str(c).strip().lower() for c in (bot.s.get("allowed_channels") or set())}
    if not allowed:
        return True
    ids = {str(chat["id"])} | ({"@" + chat["username"].lower()} if chat.get("username") else set())
    return bool(ids & allowed)


def handle_channel_membership(bot: Bot, chat: dict, member: dict) -> None:
    """Channels subscribe by making the bot an admin with "Post messages"."""
    cid = chat["id"]
    can_post = member["status"] == "creator" or (
        member["status"] == "administrator" and member.get("can_post_messages", False))
    if not can_post or not channel_allowed(bot, chat):
        set_active(bot, cid, False)
        log.info("@%s: channel %s can't post or not allowed; inactive", bot.username, cid)
        return
    prev = subs.get_item(Key={"bot": bot.username, "chat_id": cid}).get("Item")
    if prev and int(prev.get("active", 0)):
        return  # other admin rights changed; already subscribed
    subscribe(bot, chat)
    log.info("@%s: channel %s (%s) subscribed", bot.username, cid, chat.get("title"))
    if bot.enabled:
        send_with_retry(bot, cid)


def stats(bot: Bot) -> dict:
    items = query_all(subs, KeyConditionExpression=Key("bot").eq(bot.username))
    act = [i for i in items if int(i.get("active", 0))]
    return {"active": len(act), "channels": sum(i.get("type") == "channel" for i in act), "total": len(items)}


def set_image(bot: Bot, msg: dict) -> None:
    cid, uid = msg["chat"]["id"], msg["from"]["id"]
    cap = (f"New image set for @{bot.username}. It goes out from the next hourly post "
           f"({fmt(next_slot_time(time.time()))}).")
    try:
        if msg.get("photo"):
            res = api(bot.token, "sendPhoto", {"chat_id": cid, "photo": msg["photo"][-1]["file_id"], "caption": cap})
        else:
            doc = msg["document"]
            if (doc.get("file_size") or 0) > 10_000_000:
                say(bot.token, cid, "That file is over 10 MB, Telegram's photo limit. Send a smaller image.")
                return
            data = download(bot.token, doc["file_id"])
            res = api(bot.token, "sendPhoto", {"chat_id": cid, "caption": cap},
                      files={"photo": (doc.get("file_name") or "image.jpg", data,
                                       doc.get("mime_type") or "image/jpeg")}, timeout=60)
    except ApiError as e:
        say(bot.token, cid, f"Couldn't use that image: {e.desc}")
        return
    update_settings(bot.username, image_file_id=res["photo"][-1]["file_id"],
                    image_set_at=int(time.time()), image_set_by=uid)
    log.info("@%s: image changed by %s", bot.username, uid)


def handle_update(bot: Bot, upd: dict) -> None:
    mcm = upd.get("my_chat_member")
    if mcm:
        member = mcm["new_chat_member"]
        if member["status"] in ("kicked", "left"):
            set_active(bot, mcm["chat"]["id"], False)
        elif mcm["chat"]["type"] == "channel":
            handle_channel_membership(bot, mcm["chat"], member)
        return
    msg = upd.get("message")  # channel posts are ignored: channels subscribe via admin rights
    if not msg:
        return
    chat, uid = msg["chat"], (msg.get("from") or {}).get("id")
    cid, private = chat["id"], chat.get("type") == "private"
    is_image = msg.get("photo") or (msg.get("document") or {}).get("mime_type", "").startswith("image/")
    if is_image:
        if private and bot.is_admin(uid):
            set_image(bot, msg)
        return  # non-admin images are ignored
    if "text" not in msg:
        return
    cmd = msg["text"].split()[0].split("@")[0].lower()
    if cmd == "/start":
        subscribe(bot, chat)
        if not bot.enabled:
            say(bot.token, cid, "Subscribed. Reminders are paused at the moment; they'll resume automatically.")
            return
        say(bot.token, cid, "Subscribed: one reminder every hour at a random second.\n"
                            f"Next one: {fmt(next_slot_time(time.time()))}\n/stop to unsubscribe.")
        send_with_retry(bot, cid)  # first one right away; not counted as an hourly slot
    elif cmd == "/stop":
        set_active(bot, cid, False)
        say(bot.token, cid, "Unsubscribed. /start to resume.")
    elif cmd == "/next":
        say(bot.token, cid, f"Next reminder: {fmt(next_slot_time(time.time()))}")
    elif cmd == "/id":
        say(bot.token, cid, f"Your user id: {uid}" + ("" if private else f"\nThis chat: {cid}"))
    elif cmd in ("/image", "/stats") and private and bot.is_admin(uid):
        if cmd == "/stats":
            st = stats(bot)
            say(bot.token, cid, f"@{bot.username}: {st['active']} active subscribers "
                                f"({st['channels']} channels), {st['total']} total. "
                                f"{'Enabled' if bot.enabled else 'Paused'}.")
        elif bot.s.get("image_file_id"):
            when = fmt(int(bot.s.get("image_set_at", 0)))
            send_photo(bot, cid, caption=f"Current image, set {when} by {bot.s.get('image_set_by')}. "
                                         "Send a photo to replace it.")
        else:
            send_photo(bot, cid, caption="No custom image yet; this is the default. Send a photo to replace it.")


# ---------------------------------------------------------------- webhooks
def _body(event) -> dict:
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()
    return json.loads(body)


def _first_time(pk: str, upd: dict) -> bool:
    """Telegram re-delivers on timeouts; process each update once."""
    try:
        state.put_item(Item={"pk": pk, "sk": str(upd.get("update_id")), "ttl": int(time.time()) + DAY},
                       ConditionExpression="attribute_not_exists(pk)")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def find_bot(secret: str) -> Bot | None:
    if not secret:
        return None
    for force in (False, True):  # reload once in case a bot was added since the cache filled
        for u, b in bot_secrets(force).items():
            if hmac.compare_digest(secret, webhook_secret(b["token"])):
                return Bot(u, b["token"], b["admins"])
    return None


def webhook(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    bot = find_bot(headers.get("x-telegram-bot-api-secret-token", ""))
    if not bot:
        return {"statusCode": 403, "body": "forbidden"}
    upd = _body(event)
    if not _first_time(f"upd#{bot.username}", upd):
        return {"statusCode": 200, "body": "dup"}
    try:
        handle_update(bot, upd)
    except Exception:
        log.exception("@%s: update %s failed", bot.username, upd.get("update_id"))
    return {"statusCode": 200, "body": "ok"}


def admin_webhook(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    got = headers.get("x-telegram-bot-api-secret-token", "")
    tok = admin_token()
    if tok and not hmac.compare_digest(got, admin_secret(tok)):
        tok = admin_token(force=True)  # maybe rotated
    if not tok:
        return {"statusCode": 503, "body": "not configured"}
    if not hmac.compare_digest(got, admin_secret(tok)):
        return {"statusCode": 403, "body": "forbidden"}
    upd = _body(event)
    if not _first_time("upd#__admin__", upd):
        return {"statusCode": 200, "body": "dup"}
    msg = upd.get("message") or {}
    sa = super_admin()
    if sa is None or (msg.get("from") or {}).get("id") != sa or (msg.get("chat") or {}).get("type") != "private":
        return {"statusCode": 200, "body": "ignored"}  # everyone but the super admin gets silence
    try:
        handle_admin(tok, msg)
    except Exception as e:
        log.exception("admin command failed")
        say(tok, msg["chat"]["id"], f"Failed: {type(e).__name__}: {e}")
    return {"statusCode": 200, "body": "ok"}


ADMIN_HELP = """Super-admin commands:
/bots - list bots with status
/addbot <token> - add a bot (the message with the token is deleted)
/settoken <token> - replace a bot's token
/enable <bot>, /disable <bot> - resume / pause hourly posts
/removebot <bot> - remove a bot (asks to confirm)
/admins <bot> [add|del <user_id>] - show / change a bot's admins
/test <bot> - send that bot's current image to you
/id - your user id
A bot's admins set its image by sending it a photo; they can also use /image and /stats there."""


def handle_admin(tok: str, msg: dict) -> None:
    cid = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    def out(t: str) -> None:
        say(tok, cid, t)

    if not text.startswith("/"):
        return out(ADMIN_HELP)
    cmd, *args = text.split()
    cmd = cmd.split("@")[0].lower()

    if cmd in ("/addbot", "/settoken"):
        try:  # never leave a token in the chat
            api(tok, "deleteMessage", {"chat_id": cid, "message_id": msg["message_id"]})
        except ApiError as e:
            log.warning("couldn't delete token message: %s", e.desc)
        if len(args) != 1:
            return out(f"Usage: {cmd} <token>")
        new = args[0]
        try:
            me = api(new, "getMe")
        except ApiError as e:
            return out(f"Telegram rejected that token: {e.desc}")
        u = me["username"].lower()
        exists = param(f"{BOTS_PATH}/{u}/token") is not None
        if cmd == "/addbot" and exists:
            return out(f"@{me['username']} already exists. Use /settoken to replace its token.")
        if cmd == "/settoken" and not exists:
            return out(f"@{me['username']} isn't a known bot. Use /addbot.")
        put_param(f"{BOTS_PATH}/{u}/token", new)
        if param(f"{BOTS_PATH}/{u}/admins") is None:
            put_param(f"{BOTS_PATH}/{u}/admins", str(super_admin()), overwrite=False)
        status = sync_bot(u, force=True)
        verb = "added" if cmd == "/addbot" else "token updated"
        return out(f"@{me['username']} {verb}. Status: {status}")

    if cmd == "/bots":
        sec = bot_secrets(force=True)
        if not sec:
            return out("No bots yet. /addbot <token>")
        lines = []
        for u in sorted(sec):
            b = Bot(u, sec[u]["token"], sec[u]["admins"])
            st = stats(b)
            lines.append(f"@{u} - {'enabled' if b.enabled else 'PAUSED'}, {b.s.get('status', 'not synced')}\n"
                         f"  {st['active']} active ({st['channels']} channels), admins {len(b.admins)}, "
                         f"image: {'custom' if b.s.get('image_file_id') else 'default'}")
        return out("\n".join(lines))

    if cmd == "/id":
        return out(f"Your user id: {msg['from']['id']}")
    if cmd in ("/help", "/start") or not args:
        return out(ADMIN_HELP)
    bot = get_bot(args[0], force=True)
    if not bot:
        return out(f"No bot @{norm(args[0])}. /bots lists them.")

    if cmd in ("/enable", "/disable"):
        update_settings(bot.username, enabled=(cmd == "/enable"))
        return out(f"@{bot.username} {'enabled' if cmd == '/enable' else 'paused'}.")
    if cmd == "/removebot":
        if args[1:2] != ["yes"]:
            st = stats(bot)
            return out(f"Remove @{bot.username} ({st['active']} active subscribers)? Its webhook and token are "
                       f"deleted; subscribers are kept in case you add it back.\n"
                       f"Confirm: /removebot {bot.username} yes")
        try:
            api(bot.token, "deleteWebhook")
        except ApiError as e:
            log.warning("deleteWebhook failed: %s", e.desc)
        ssm.delete_parameters(Names=[f"{BOTS_PATH}/{bot.username}/token", f"{BOTS_PATH}/{bot.username}/admins"])
        bots.delete_item(Key={"username": bot.username})
        bot_secrets(force=True)
        return out(f"@{bot.username} removed.")
    if cmd == "/admins":
        if len(args) == 1:
            return out(f"@{bot.username} admins: {', '.join(map(str, sorted(bot.admins))) or 'none'}"
                       " (you can manage every bot regardless)")
        if len(args) != 3 or args[1] not in ("add", "del") or not re.fullmatch(r"\d+", args[2]):
            return out("Usage: /admins <bot> [add|del <user_id>]")
        ids = set(bot.admins)
        (ids.add if args[1] == "add" else ids.discard)(int(args[2]))
        put_param(f"{BOTS_PATH}/{bot.username}/admins", ",".join(map(str, sorted(ids))) or " ")
        bot_secrets(force=True)
        set_commands(bot.token, ids)
        return out(f"@{bot.username} admins: {', '.join(map(str, sorted(ids))) or 'none'}")
    if cmd == "/test":
        try:
            send_photo(bot, cid, caption=f"Test from @{bot.username}")
            return out("Sent.")
        except ApiError as e:
            hint = f" Open @{bot.username} and press Start first." if e.gone else ""
            return out(f"@{bot.username} couldn't message you: {e.desc}.{hint}")
    return out(ADMIN_HELP)


# ---------------------------------------------------------------- sync
def fingerprint(secret: str, url: str) -> str:
    return hashlib.sha256(f"{url}|{secret}|{','.join(ALLOWED_UPDATES)}".encode()).hexdigest()[:16]


def ensure_webhook(token: str, url: str, secret: str, force: bool = False, registered: str | None = None) -> str:
    """Point a bot's webhook at url. Never raises: returns ok / set / error: ...
    `registered` is the fingerprint of what we last registered for this bot. A new
    token means a new secret, hence a new fingerprint, so rotation re-registers
    without needing force; concurrent syncs of an unchanged bot don't call setWebhook."""
    if not url:
        return "no webhook URL"
    try:
        if not force:
            info = api(token, "getWebhookInfo")
            if (info.get("url") == url and sorted(info.get("allowed_updates") or []) == sorted(ALLOWED_UPDATES)
                    and "403" not in (info.get("last_error_message") or "")  # 403 = stale secret
                    and registered in (None, fingerprint(secret, url))):
                return "ok"
        api(token, "setWebhook", {"url": url, "secret_token": secret, "allowed_updates": ALLOWED_UPDATES,
                                  "drop_pending_updates": False})
        return "set"
    except Exception as e:
        log.error("webhook registration failed: %s", e)
        return f"error: {e}"


def set_commands(token: str, admins: set[int]) -> None:
    try:
        api(token, "setMyCommands", {"commands": USER_COMMANDS})
    except Exception as e:
        log.warning("setMyCommands failed: %s", e)
        return
    for a in admins:  # admins see the extra commands in their private chat with the bot
        try:
            api(token, "setMyCommands", {"commands": BOT_ADMIN_COMMANDS, "scope": {"type": "chat", "chat_id": a}})
        except Exception:
            pass  # admin hasn't started the bot yet


def sync_bot(u: str, force: bool = False, commands: bool = False) -> str:
    """Validate a bot's token, make sure its webhook is ours, record status in DynamoDB."""
    u = norm(u)
    b = bot_secrets(force=True).get(u)
    now = int(time.time())
    if not b:
        update_settings(u, create=False, status="token missing", checked_at=now)
        return "token missing"
    try:
        me = api(b["token"], "getMe")
    except ApiError as e:
        status = f"invalid token: {e.desc}"
        update_settings(u, status=status, checked_at=now)
        return status
    if me["username"].lower() != u:
        status = f"token belongs to @{me['username']}, not @{u}"
        update_settings(u, status=status, checked_at=now)
        return status
    secret = webhook_secret(b["token"])
    fp = fingerprint(secret, WEBHOOK_URL)
    hook = ensure_webhook(b["token"], WEBHOOK_URL, secret, force, settings(u).get("hook_fp", fp))
    if force or hook == "set" or commands:
        set_commands(b["token"], b["admins"] | ({super_admin()} - {None}))
    status = "ok" if hook in ("ok", "set") else hook
    extra = {"hook_fp": fp} if hook in ("ok", "set") else {}
    update_settings(u, status=status, checked_at=now, bot_id=me["id"], display_name=me.get("first_name", ""), **extra)
    log.info("@%s synced: %s (webhook %s)", u, status, hook)
    return status


def sync_all(force: bool = False) -> dict[str, str]:
    sec = bot_secrets(force=True)
    res = {u: sync_bot(u, force) for u in sec}
    for item in scan_all(bots, ProjectionExpression="username"):
        if item["username"] not in sec:
            res[item["username"]] = sync_bot(item["username"])  # -> token missing
    return res


def ensure_admin_bot(force: bool = False) -> str:
    tok = admin_token(force=True)
    if not tok:
        return "admin token missing"
    secret = admin_secret(tok)
    fp = fingerprint(secret, ADMIN_WEBHOOK_URL)
    prev = (state.get_item(Key={"pk": "meta", "sk": "admin-hook"}).get("Item") or {}).get("fp", fp)
    hook = ensure_webhook(tok, ADMIN_WEBHOOK_URL, secret, force, prev)
    if force or hook == "set":
        try:
            api(tok, "setMyCommands", {"commands": SUPER_COMMANDS})
        except Exception as e:
            log.warning("admin setMyCommands failed: %s", e)
    if hook in ("ok", "set"):
        state.put_item(Item={"pk": "meta", "sk": "admin-hook", "fp": fp})
    return "ok" if hook in ("ok", "set") else hook


def on_ssm_change(detail: dict) -> dict:
    name, op = detail.get("name", ""), detail.get("operation", "")
    log.info("SSM %s %s", op, name)
    if name == ADMIN_TOKEN_PARAM:
        return {"admin": ensure_admin_bot()}
    if name == SUPER_ADMIN_PARAM:
        super_admin(force=True)
        return {"super_admin": "reloaded"}
    if name.startswith(BOTS_PATH + "/"):
        parts = name[len(BOTS_PATH) + 1:].split("/")
        if len(parts) == 2:
            return {parts[0]: sync_bot(parts[0], commands=(parts[1] == "admins"))}
    return {}


def migrate_legacy() -> str:
    """One-off move of the original single bot (SSM {P}/token + users table) into the
    multi-bot layout. Idempotent; never overwrites newer data."""
    old = param(LEGACY_TOKEN_PARAM)
    if not old:
        return "no legacy bot"
    try:
        me = api(old, "getMe")
    except ApiError as e:
        return f"legacy token rejected: {e.desc}"
    u = me["username"].lower()
    if param(f"{BOTS_PATH}/{u}/token") is None:
        put_param(f"{BOTS_PATH}/{u}/token", old, overwrite=False)
    sa = super_admin(force=True)
    if param(f"{BOTS_PATH}/{u}/admins") is None and sa:
        put_param(f"{BOTS_PATH}/{u}/admins", str(sa), overwrite=False)
    bot_secrets(force=True)
    n = 0
    if LEGACY_USERS_TABLE and "Item" not in state.get_item(Key={"pk": "meta", "sk": "migrated-users"}):
        for i in scan_all(ddb.Table(LEGACY_USERS_TABLE)):
            item = {"bot": u, "chat_id": int(i["chat_id"]), "active": int(i.get("active", 0)),
                    "subscribed_at": int(i.get("subscribed_at", 0)), "type": i.get("type") or "private",
                    "name": i.get("name") or ""}
            try:
                subs.put_item(Item=item, ConditionExpression="attribute_not_exists(chat_id)")
                n += 1
            except ClientError as e:
                if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
        state.put_item(Item={"pk": "meta", "sk": "migrated-users", "n": n, "bot": u})
    photo = state.get_item(Key={"pk": "meta", "sk": "photo"}).get("Item")
    if photo and not settings(u).get("default_file_id"):
        update_settings(u, default_file_id=photo["file_id"], default_sha=photo["sha"])
    return f"@{u}: {n} subscribers migrated"


# ---------------------------------------------------------------- planner
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
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": {k: str(v)[:500] for k, v in (data or {}).items()}}).encode()
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=30).read()


def _cfn(event, context):
    """Custom::PostDeploy: set up on create/update, clean up on delete.
    Always answers CloudFormation, or the stack would hang for an hour."""
    try:
        kind = event["RequestType"]
        if kind == "Delete":
            removed = delete_slot_schedules()
            for u, b in bot_secrets(force=True).items():
                try:
                    api(b["token"], "deleteWebhook")
                except Exception as e:
                    log.warning("@%s deleteWebhook failed: %s", u, e)
            tok = admin_token(force=True)
            if tok:
                try:
                    api(tok, "deleteWebhook")
                except Exception as e:
                    log.warning("admin deleteWebhook failed: %s", e)
            data = {"Schedules": f"{removed} deleted", "Webhook": "deleted"}
        else:
            new, old = event.get("ResourceProperties", {}), event.get("OldResourceProperties", {})
            removed = 0
            if kind == "Update" and str(new.get("CatchupGrace")) != str(old.get("CatchupGrace")):
                removed = delete_slot_schedules()  # recreate with the new retry window
            migrated = migrate_legacy()
            created = plan(time.time())
            synced = sync_all(force=True)
            data = {"Schedules": f"{removed} recreated, {len(created)} created", "Migration": migrated,
                    "Webhook": ", ".join(f"@{u}: {s}" for u, s in synced.items()) or "no bots",
                    "AdminBot": ensure_admin_bot(force=True)}
        log.info("custom resource %s: %s", kind, data)
        _cfn_respond(event, context, "SUCCESS", json.dumps(data), data)
    except Exception as e:
        log.exception("custom resource failed")
        _cfn_respond(event, context, "FAILED", f"{type(e).__name__}: {e}")


def planner(event, context):
    event = event or {}
    if "RequestType" in event and "ResponseURL" in event:
        return _cfn(event, context)
    if event.get("source") == "aws.ssm":
        return on_ssm_change(event.get("detail") or {})
    return {"created": plan(time.time()), "bots": sync_all(), "admin": ensure_admin_bot()}


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
        # broadcast then judges lateness against "at".
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


# ---------------------------------------------------------------- broadcast
def _invoke(payload: dict) -> None:
    lambda_.invoke(FunctionName=BROADCAST_ARN, InvocationType="Event", Payload=json.dumps(payload).encode())


def broadcast(event, context):
    slot = int(event["slot"])
    t = slot_time(slot)
    due = max(t, int(event.get("at", t)))  # "at" > t only for slots pushed out by the planner
    late = time.time() - due
    if late > GRACE and not event.get("continuation"):
        log.warning("slot %d would start %ds late (> grace %ds); dropping", slot, late, GRACE)
        return {"skipped": "stale"}
    if "bot" not in event:  # fired by the schedule: one invocation per enabled bot
        fanned = []
        for u in sorted(bot_secrets(force=True)):
            if settings(u).get("enabled", True) is False:
                continue
            _invoke({"slot": slot, "at": due, "bot": u})
            fanned.append(u)
        log.info("slot %d (%s): fanned out to %s", slot, fmt(t), fanned)
        return {"bots": fanned}

    bot = get_bot(event["bot"], force=True)
    if not bot or not bot.enabled:
        return {"skipped": "no such bot or paused"}
    u = bot.username
    if "Item" in state.get_item(Key={"pk": f"done#{u}", "sk": str(slot)}):
        return {"skipped": "done"}
    wait = t - time.time()
    if wait > context.get_remaining_time_in_millis() / 1000 - 120:
        raise RuntimeError(f"invoked {wait:.0f}s before slot {slot}; refusing")
    if wait > 0:
        time.sleep(wait)
    lease_until = int(time.time() + context.get_remaining_time_in_millis() / 1000 + 30)
    try:
        state.put_item(Item={"pk": f"lock#{u}", "sk": str(slot), "until": lease_until, "ttl": lease_until + DAY},
                       ConditionExpression="attribute_not_exists(pk) OR #u < :now",
                       ExpressionAttributeNames={"#u": "until"},
                       ExpressionAttributeValues={":now": int(time.time())})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            log.info("@%s slot %d: another run holds the lease", u, slot)
            return {"skipped": "locked"}
        raise
    try:
        result = _broadcast_locked(bot, slot, t, context)
    finally:
        state.delete_item(Key={"pk": f"lock#{u}", "sk": str(slot)})
    if result.get("continued"):  # only after releasing the lease, so the continuation can take it
        _invoke({"slot": slot, "bot": u, "continuation": True})
    return result


def _broadcast_locked(bot: Bot, slot: int, t: int, context) -> dict:
    u = bot.username
    delivered = {int(i["sk"]) for i in query_all(
        state, KeyConditionExpression=Key("pk").eq(f"sent#{u}#{slot}"), ProjectionExpression="sk",
        ConsistentRead=True)}
    pending = sorted(int(i["chat_id"]) for i in query_all(
        subs, KeyConditionExpression=Key("bot").eq(u),
        FilterExpression=Attr("active").eq(1) & Attr("subscribed_at").lte(t), ProjectionExpression="chat_id")
        if int(i["chat_id"]) not in delivered)
    log.info("@%s slot %d (%s): %d to send, %d already delivered", u, slot, fmt(t), len(pending), len(delivered))
    sent = failed = 0
    for chat_id in pending:  # once started, a round always finishes (grace only gates the start)
        if context.get_remaining_time_in_millis() < 30_000:
            log.info("@%s slot %d: low on time after %d sends; continuing", u, slot, sent)
            return {"sent": sent, "failed": failed, "continued": True}
        if send_with_retry(bot, chat_id):
            state.put_item(Item={"pk": f"sent#{u}#{slot}", "sk": str(chat_id), "ttl": int(time.time()) + 2 * DAY})
            sent += 1
        else:
            failed += 1
        time.sleep(1 / SEND_RATE)
    state.put_item(Item={"pk": f"done#{u}", "sk": str(slot), "ttl": int(time.time()) + 2 * DAY})
    log.info("@%s slot %d: done, sent %d, failed %d", u, slot, sent, failed)
    return {"sent": sent, "failed": failed}

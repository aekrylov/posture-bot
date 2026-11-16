#!/bin/sh
# Health check for the deployed multi-bot posture-bot. Never prints a token.
# Needs: aws CLI with access to the stack's region, python3.
R=${AWS_REGION:-eu-central-1} P=${SSM_PREFIX:-/posture-bot} exec python3 - "$@" <<'EOF'
import json, os, re, subprocess, sys, time, urllib.request

R, P = os.environ["R"], os.environ["P"]
FAIL = False
G, X, N = "\033[32m✔\033[0m", "\033[31m✘\033[0m", "\033[0m"


def ok(m): print(f"  {G} {m}")
def bad(m):
    global FAIL
    FAIL = True
    print(f"  {X} {m}")


def aws(*args):
    r = subprocess.run(["aws", "--region", R, "--output", "json", *args], capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr.strip().splitlines()[-1] if r.stderr.strip() else "aws failed")
    return json.loads(r.stdout or "null")


def tg(token, method):
    try:
        with urllib.request.urlopen(f"https://api.telegram.org/bot{token}/{method}", timeout=15) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        try:
            return json.load(e)
        except Exception:
            return {"ok": False, "description": f"HTTP {e.code}"}


def check_hook(token, want):
    r = tg(token, "getWebhookInfo").get("result", {})
    if r.get("url") == want:
        ok("webhook points at our Lambda URL")
    else:
        bad(f"webhook url is {r.get('url')!r}, expected {want} (sam deploy, or wait for the hourly planner)")
    if r.get("pending_update_count"):
        print(f"    pending updates: {r['pending_update_count']}")
    if r.get("last_error_date") and time.time() - r["last_error_date"] < 6 * 3600:
        bad(f"delivery error {int(time.time() - r['last_error_date'])}s ago: {r.get('last_error_message')}")


outs = {o["OutputKey"]: o["OutputValue"] for o in aws("cloudformation", "describe-stacks", "--stack-name",
                                                       "posture-bot")["Stacks"][0]["Outputs"]}
params = {}
for p in aws("ssm", "get-parameters-by-path", "--path", P, "--recursive", "--with-decryption")["Parameters"]:
    params[p["Name"]] = p["Value"].strip()
bots = sorted({n.split("/")[-2] for n in params if n.startswith(f"{P}/bots/") and n.endswith("/token")})
settings = {i["username"]["S"]: i for i in aws("dynamodb", "scan", "--table-name", "posture-bot-bots")["Items"]}
subs = aws("dynamodb", "scan", "--table-name", "posture-bot-subs")["Items"]

print("1. Super admin")
sa = params.get(f"{P}/super-admin", "")
ok("super-admin id stored") if re.fullmatch(r"\d+", sa) else bad(f"{P}/super-admin missing or not a user id")
tok = params.get(f"{P}/admin-token")
if not tok:
    bad(f"{P}/admin-token not stored yet")
else:
    me = tg(tok, "getMe")
    ok(f"admin bot token valid: @{me['result']['username']}") if me.get("ok") else bad(f"admin token: {me.get('description')}")
    if me.get("ok"):
        check_hook(tok, outs["AdminWebhookUrl"])
tok = None

print(f"2. Bots ({len(bots)})")
for u in bots:
    tok = params[f"{P}/bots/{u}/token"]
    s = settings.get(u, {})
    enabled = s.get("enabled", {}).get("BOOL", True)
    status = s.get("status", {}).get("S", "not synced")
    mine = [x for x in subs if x["bot"]["S"] == u]
    act = [x for x in mine if x.get("active", {}).get("N") == "1"]
    chans = sum(x.get("type", {}).get("S") == "channel" for x in act)
    admins = [a for a in re.split(r"[,\s]+", params.get(f"{P}/bots/{u}/admins", "")) if a]
    print(f"  @{u}: {'enabled' if enabled else 'PAUSED'}, {len(act)} active ({chans} channels) / {len(mine)} total, "
          f"{len(admins)} admins, image {'custom' if 'image_file_id' in s else 'default'}")
    ok("status ok") if status == "ok" else bad(f"status: {status}")
    me = tg(tok, "getMe")
    if not me.get("ok"):
        bad(f"getMe: {me.get('description')}")
    elif me["result"]["username"].lower() != u:
        bad(f"token belongs to @{me['result']['username']}")
    else:
        check_hook(tok, outs["WebhookUrl"])
tok = None
for u in sorted(set(settings) - set(bots)):
    bad(f"@{u} has settings but no token in SSM")

print("3. Schedule")
n = len(aws("scheduler", "list-schedules", "--group-name", "posture-bot", "--name-prefix", "slot-")["Schedules"])
ok(f"{n} upcoming hourly schedules") if n >= 20 else bad(f"only {n} upcoming schedules")
st = aws("scheduler", "get-schedule", "--name", "posture-bot-planner")["State"]
ok("hourly planner enabled") if st == "ENABLED" else bad(f"planner schedule {st}")
legacy = f"{P}/token" in params
if legacy:
    print(f"    note: legacy {P}/token still exists (safe to delete once the migration is verified)")

print("4. Deliveries & errors (last 6h)")
ev = aws("logs", "filter-log-events", "--log-group-name", "/aws/lambda/posture-bot",
         "--start-time", str(int((time.time() - 6 * 3600) * 1000)))["events"]
msgs = [e["message"].split("\t")[-1].strip() for e in ev]
for d in [m for m in msgs if ": done, sent" in m][-8:]:
    print("    " + d)
errs = [m[:160] for m in msgs if re.search(r"\[ERROR\]|Traceback|Task timed out|failed:", m)]
ok("no errors") if not errs else bad(f"{len(errs)} error lines, latest:")
for e in errs[-3:]:
    print("      " + e.replace("\n", " "))
url = aws("sqs", "get-queue-url", "--queue-name", "posture-bot-dlq")["QueueUrl"]
dlq = aws("sqs", "get-queue-attributes", "--queue-url", url, "--attribute-names",
          "ApproximateNumberOfMessages")["Attributes"]["ApproximateNumberOfMessages"]
ok("dead-letter queue empty") if dlq == "0" else bad(f"{dlq} missed slot events in posture-bot-dlq")
sys.exit(1 if FAIL else 0)
EOF

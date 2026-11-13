#!/bin/sh
# Health check for the deployed posture-bot. Never prints the token.
set -u
R=${AWS_REGION:-eu-central-1}
ok() { printf '  \033[32m✔\033[0m %s\n' "$*"; }
bad() { printf '  \033[31m✘\033[0m %s\n' "$*"; FAIL=1; }
FAIL=0

echo "1. Token in SSM"
TOKEN=$(aws ssm get-parameter --region "$R" --name /posture-bot/token --with-decryption \
  --query Parameter.Value --output text 2>/dev/null) || { bad "no /posture-bot/token"; exit 1; }
echo "$TOKEN" | grep -Eq '^[0-9]+:[A-Za-z0-9_-]{30,}$' && ok "format looks like a bot token" \
  || bad "doesn't look like a bot token (got ${#TOKEN} chars)"

echo "2. Telegram"
ME=$(curl -s "https://api.telegram.org/bot$TOKEN/getMe")
echo "$ME" | grep -q '"ok":true' && ok "token valid: @$(echo "$ME" | python3 -c 'import json,sys;print(json.load(sys.stdin)["result"]["username"])')" \
  || bad "getMe failed: $ME"
URL=$(aws cloudformation describe-stacks --region "$R" --stack-name posture-bot \
  --query "Stacks[0].Outputs[?OutputKey=='WebhookUrl'].OutputValue" --output text)
curl -s "https://api.telegram.org/bot$TOKEN/getWebhookInfo" | python3 -c "
import json,sys,time
r=json.load(sys.stdin).get('result',{}); want='$URL'
print('  \033[32m✔\033[0m webhook points at the Lambda URL' if r.get('url')==want else f'  \033[31m✘\033[0m webhook url is {r.get(\"url\")!r}, expected {want} (run sam deploy, or wait for the hourly planner)')
print(f'    pending updates: {r.get(\"pending_update_count\",0)}')
if r.get('last_error_date'):
    print(f'  \033[31m✘\033[0m last delivery error {int(time.time()-r[\"last_error_date\"])}s ago: {r.get(\"last_error_message\")}')
else:
    print('  \033[32m✔\033[0m no webhook delivery errors')"
unset TOKEN

echo "3. Subscribers"
aws dynamodb scan --region "$R" --table-name posture-bot-users --output json \
  --query 'Items[].{id:chat_id.N,active:active.N,name:name.S,type:type.S}' | python3 -c "
import json,sys; u=json.load(sys.stdin)
ch=[x for x in u if x['type']=='channel' and x['active']=='1']
print(f'    {sum(x[\"active\"]==\"1\" for x in u)} active / {len(u)} total ({len(ch)} active channels)')
for x in u[:10]: print(f'    {x[\"id\"]:>15}  active={x[\"active\"]}  {x[\"type\"] or \"private\":8} {x[\"name\"] or \"\"}')"

echo "4. Schedule"
aws scheduler list-schedules --region "$R" --group-name posture-bot --name-prefix slot- --output json \
  --query 'Schedules[].Name' | python3 -c "
import json,sys; n=sorted(int(s.split('-')[1]) for s in json.load(sys.stdin))
print(f'  \033[32m✔\033[0m {len(n)} upcoming hourly schedules' if len(n)>=20 else f'  \033[31m✘\033[0m only {len(n)} upcoming schedules')"
aws scheduler get-schedule --region "$R" --name posture-bot-planner --query State --output text | grep -q ENABLED \
  && ok "hourly planner enabled" || bad "planner schedule not enabled"

echo "5. Deliveries & errors (last 6h)"
aws logs filter-log-events --region "$R" --log-group-name /aws/lambda/posture-bot \
  --start-time $(( ($(date +%s) - 21600) * 1000 )) --output json \
  --query 'events[].message' | python3 -c "
import json,sys,re
m=json.load(sys.stdin)
done=[x.split('\t')[-1].strip() for x in m if ': done, sent' in x]
errs=[x.split('\t')[-1].strip()[:160] for x in m if re.search(r'\[ERROR\]|Traceback|Task timed out|failed:', x)]
for d in done[-6:]: print('    '+d)
print(f'  \033[32m✔\033[0m no errors' if not errs else f'  \033[31m✘\033[0m {len(errs)} error lines, latest:')
for e in errs[-3:]: print('      '+e)"
DLQ=$(aws sqs get-queue-attributes --region "$R" --attribute-names ApproximateNumberOfMessages \
  --queue-url "$(aws sqs get-queue-url --region "$R" --queue-name posture-bot-dlq --query QueueUrl --output text)" \
  --query Attributes.ApproximateNumberOfMessages --output text)
[ "$DLQ" = 0 ] && ok "dead-letter queue empty" || bad "$DLQ missed slot events in posture-bot-dlq"
exit $FAIL

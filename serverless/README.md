# posture-bot: serverless version

Lambda, EventBridge Scheduler and DynamoDB. It does the same job as `../bot.py`, but AWS handles the scheduling.

```
EventBridge Scheduler ── rate(1h) ──▶ planner ──creates──▶ one-time at() schedule per hour (24h ahead)
                                                                │ fires at the slot's minute
Telegram ──webhook──▶ function URL ──▶ webhook                  ▼
                                         │                   broadcast ── sleeps to the exact second,
                                         ▼                      │         sends to all active users
                                 DynamoDB users / state  ◀──────┘
```

## Deploy

```sh
aws ssm put-parameter --name /posture-bot/token --type SecureString --value '<BotFather token>'
./deploy.sh
```

`deploy.sh` does three things:

- deploys the stack with `sam deploy`
- runs the planner once
- registers the Telegram webhook, if the token parameter exists

The webhook is registered by the planner Lambda itself, so the token stays inside AWS. To set options, re-run the script with `PARAMS`, for example:

```sh
PARAMS="Caption='Выпрями спину' CatchupGrace=1800" ./deploy.sh
```

## Outage handling

- **Schedules exist well ahead.** One-time schedules are created 24 hours in advance, so the planner can fail for a day without any slot being missed. AWS invokes each one itself, retrying for up to `CatchupGrace` seconds. After that the event goes to the `posture-bot-dlq` SQS queue.
- **Planner catches up.** If a slot is overdue but still within grace and hasn't been delivered, for example because its schedule was lost or fired while Lambda was failing, the hourly planner invokes broadcast directly.
- **Broadcast is idempotent.** It keeps a per-(slot, chat) delivery log, holds a per-slot lease, and writes a done marker when finished. Scheduler retries, Lambda async retries and planner catch-ups therefore never cause duplicates.
- **Slot times are fixed.** Each hour's second is `HMAC(seed, slot) mod 3600`, and the seed is stored in DynamoDB.
- **Long broadcasts continue.** A broadcast that runs low on Lambda time hands off to a fresh invocation. One invocation sends to about 20k users at 25 msg/s.
- **New users get a photo immediately.** `/start` sends one right away; it doesn't count as an hourly slot.

## Operations

- Logs: CloudWatch `/aws/lambda/posture-bot` (14-day retention)
- Upcoming slots: `aws scheduler list-schedules --group-name posture-bot`
- Missed events: `aws sqs receive-message --queue-url <DeadLetterQueueUrl output>`
- Tear down: `sam delete --stack-name posture-bot`

## Cost

About $1/month for 1000 users, mostly DynamoDB delivery-log writes.

## Test

The local test uses moto to fake AWS and a fake Telegram API.

```sh
python3.12 -m venv .venv && .venv/bin/pip install 'moto[dynamodb,ssm,scheduler]' boto3
.venv/bin/python test_local.py
```

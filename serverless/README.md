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
PARAMS="Caption='Выпрями спину' CatchupGrace=300" ./deploy.sh
```

## Posting to a channel

1. Create the channel, public or private.
2. Open **Channel → Administrators → Add admin** and pick the bot.
3. Leave **Post messages** enabled. No other rights are needed.

The bot subscribes the channel as soon as it gets admin rights and posts the first photo straight away. It then posts every hour, the same as for private chats.

To unsubscribe the channel, remove the bot or take away its "Post messages" right. Text commands are never posted to a channel.

By default, any channel that makes the bot an admin gets subscribed. To limit it to your own channels:

```sh
PARAMS="AllowedChannels=@mychannel,-1001234567890" ./deploy.sh
```

## Outage handling

- **Schedules exist well ahead.** One-time schedules are created 24 hours in advance, so the planner can fail for a day without any slot being missed. AWS invokes each one itself, retrying for up to `CatchupGrace` seconds (default 120). After that the event goes to the `posture-bot-dlq` SQS queue.
- **No late spam.** A slot that can't start within `CatchupGrace` of its time is dropped, never sent late, and the planner never re-sends past slots. A round that has started always finishes, however long it takes.
- **Too-close slots are pushed out.** If a slot is too close to schedule at its own minute (only right after the first deploy), its schedule is set 90 s from now instead. Lateness is then measured from that new time.
- **Broadcast is idempotent.** It keeps a per-(slot, chat) delivery log, holds a per-slot lease, and writes a done marker when finished. Scheduler retries and Lambda async retries therefore never cause duplicates.
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

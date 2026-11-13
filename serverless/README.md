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
sam deploy
```

`samconfig.toml` holds the stack name, region and capabilities, so no flags are needed. On every create or update, the `PostDeploy` custom resource runs the planner, which does two things:

- schedules the next 24 hours of slots
- registers the Telegram webhook from inside AWS, so the token never leaves it

Deleting the stack (`sam delete`) removes the schedules and the webhook.

If the token isn't stored yet, the deploy still succeeds. The hourly planner registers the webhook within an hour of the token appearing. It also re-registers it if the URL changes, or if the token is rotated and Telegram starts getting 403s.

Options are stack parameters. SAM keeps a parameter's previous value until you override it:

```sh
sam deploy --parameter-overrides "Caption='Выпрями спину' CatchupGrace=300"
```

## Posting to a channel

1. Create the channel, public or private.
2. Open **Channel → Administrators → Add admin** and pick the bot.
3. Leave **Post messages** enabled. No other rights are needed.

The bot subscribes the channel as soon as it gets admin rights and posts the first photo straight away. It then posts every hour, the same as for private chats.

To unsubscribe the channel, remove the bot or take away its "Post messages" right. Text commands are never posted to a channel.

By default, any channel that makes the bot an admin gets subscribed. To limit it to your own channels:

```sh
sam deploy --parameter-overrides "AllowedChannels=@mychannel,-1001234567890"
```

## Outage handling

- **Schedules exist well ahead.** One-time schedules are created 24 hours in advance, so the planner can fail for a day without any slot being missed. AWS invokes each one itself, retrying for up to `CatchupGrace` seconds (default 120). After that the event goes to the `posture-bot-dlq` SQS queue.
- **No late spam.** A slot that can't start within `CatchupGrace` of its time is dropped, never sent late, and the planner never re-sends past slots. A round that has started always finishes, however long it takes.
- **Too-close slots are pushed out.** If a slot is too close to schedule at its own minute (only right after the first deploy), its schedule is set 90 s from now instead. Lateness is then measured from that new time.
- **Broadcast is idempotent.** It keeps a per-(slot, chat) delivery log, holds a per-slot lease, and writes a done marker when finished. Retries resend only to chats without a recorded delivery. The one gap: a crash or timeout between Telegram accepting a photo and the delivery being recorded resends to that one chat.
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

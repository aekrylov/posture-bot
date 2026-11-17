# posture-bot: serverless, multi-bot

For setup and usage, see the [root README](../README.md). This file covers the design.

Lambda, EventBridge Scheduler, DynamoDB and SSM. Any number of Telegram bots do the same thing: once an hour, each sends its own image to its subscribers, all at the same random second. A separate super-admin bot manages them.

```
EventBridge Scheduler ── rate(1h) ──▶ planner ──creates──▶ one-time at() schedule per hour (24h ahead)
SSM parameter change ───────────────▶   │  syncs bots                │ fires at the slot's minute
                                        ▼                            ▼
Telegram (all bots) ──▶ shared URL ──▶ webhook          broadcast ── one invocation per bot,
Telegram (admin bot) ─▶ admin URL ───▶ admin_webhook        │        sleeps to the exact second
                                        │                    │
                            DynamoDB bots / subs / state  ◀──┘     SSM: tokens, admins
```

## Where things live

- **SSM Parameter Store** (all SecureString) holds credentials and access:
  - `/posture-bot/bots/<username>/token`: a bot's BotFather token. Whether this parameter exists decides whether the bot exists.
  - `/posture-bot/bots/<username>/admins`: comma-separated Telegram user IDs of that bot's admins.
  - `/posture-bot/admin-token`: token of the super-admin bot.
  - `/posture-bot/super-admin`: your Telegram user ID. It's kept out of the code because the repo is public.
- **DynamoDB:**
  - `posture-bot-bots`, one item per bot. You can edit `enabled`, `caption` and `allowed_channels` (a string set) in the console. `status`, `checked_at` and the image `file_id`s are filled in by the system. Tokens are never stored here.
  - `posture-bot-subs`: subscribers, keyed by (bot, chat_id).
  - `posture-bot-state`: the schedule seed, per-bot locks, delivery logs and update dedupe.

## Deploy

```sh
aws ssm put-parameter --name /posture-bot/super-admin --type SecureString --value '<your Telegram user id>'
aws ssm put-parameter --name /posture-bot/admin-token --type SecureString --value '<admin bot token>'
sam deploy
```

On every create or update, the `PostDeploy` custom resource runs the planner. It does four things:

- moves the original single bot into the multi-bot layout (once)
- schedules the next 24 hours
- validates every bot and registers its webhook
- registers the admin bot's webhook

The parameters can be stored before or after the deploy. Any change under `/posture-bot/` syncs the affected bot within seconds through EventBridge, and the hourly planner re-checks everything as well.

## Managing bots

From the super-admin bot (only the ID in `/posture-bot/super-admin` gets answers):

- `/addbot <token>`: checks the token and stores it in SSM under the bot's username, with you as admin. It then registers the webhook and commands, and deletes your message so the token doesn't stay in the chat.
- `/settoken <token>`: replaces a bot's token after rotating it in BotFather.
- `/bots`: lists every bot with its status, subscriber counts and image.
- `/enable <bot>`, `/disable <bot>`: resume or pause a bot's hourly posts.
- `/removebot <bot>`: asks for confirmation, then deletes the webhook, the SSM parameters and the bot's settings. Subscribers are kept.
- `/admins <bot> [add|del <user_id>]`: shows or changes a bot's admins.
- `/test <bot>`: sends that bot's current image to you. You need to have pressed Start in that bot first.

From the AWS console instead:

- **Add a bot:** create `/posture-bot/bots/<username>/token` in SSM, with `<username>` in lowercase.
- **Change admins:** edit `/posture-bot/bots/<username>/admins`.

Mistakes show up within seconds in the bot's `status` in `posture-bot-bots`, e.g. "invalid token" or "token belongs to @other_bot".

## Per bot

- **Setting the image:** a bot admin (or you) sends the bot a photo in a private chat, and it becomes the bot's image. Telegram already holds the file, so its `file_id` is stored right away and reused for every send. A preview is sent back from that `file_id`. An image sent as a file is downloaded and re-uploaded as a photo. Photos from non-admins are ignored.
- **Default image:** until an image is set, the bot sends the bundled `photo.jpg`. It's uploaded once per bot, then reused by `file_id`.
- **Commands:** everyone gets `/start` (subscribe, with a photo straight away), `/stop`, `/next` and `/id`. Admins also get `/image` and `/stats`, which appear in their command menu.
- **Channels:** make the bot a channel admin with "Post messages" and the channel is subscribed to that bot. `allowed_channels` in the bot's item restricts which channels can do this.

## Outage handling

- **Schedules exist well ahead.** One-time schedules are created 24 hours in advance, so the planner can fail for a day without any slot being missed. AWS retries each one for up to `CatchupGrace` seconds (default 120), then sends the event to the `posture-bot-dlq` SQS queue.
- **Bots send independently.** A slot fans out one async invocation per enabled bot. Each bot has its own lock, delivery log and done marker, so one failing bot doesn't affect the others.
- **No late spam.** A slot that can't start within `CatchupGrace` of its time is dropped, never sent late. A round that has started always finishes.
- **Retries.** Retries resend only to chats without a recorded delivery. The one gap: a crash or timeout between Telegram accepting a photo and the delivery being recorded resends to that one chat.
- **Concurrency limit.** This account's Lambda limit is 10 concurrent executions, so sending gets throttled at the top of the hour with more than about 8 bots. Raising the limit is a free request in Service Quotas.

## Operations

- Health check (never prints tokens): `./check.sh`
- Logs: CloudWatch `/aws/lambda/posture-bot` (14-day retention)
- Missed events: `aws sqs receive-message --queue-url <DeadLetterQueueUrl output>`
- Tear down: `sam delete`. This removes the schedules and every webhook. The SSM parameters and the legacy `posture-bot-users` table are kept.

## Cost

About $1/month per 1000 subscribers, mostly DynamoDB delivery-log writes. SSM standard parameters are free.

## Test

The local test uses moto to fake AWS and a fake multi-token Telegram API.

```sh
python3.12 -m venv .venv && .venv/bin/pip install 'moto[dynamodb,ssm,scheduler]' boto3
.venv/bin/python test_local.py
```

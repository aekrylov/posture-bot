#!/bin/sh
# Deploy posture-bot serverless stack, run the planner once, and register the
# Telegram webhook if the token is already in SSM (/posture-bot/token).
set -eu
cd "$(dirname "$0")"
STACK=${STACK:-posture-bot}
REGION=${AWS_REGION:-$(aws configure get region)}
rm -rf src/__pycache__
sam validate --lint --region "$REGION"
sam deploy --template-file template.yaml --stack-name "$STACK" --region "$REGION" \
  --resolve-s3 --capabilities CAPABILITY_IAM --no-confirm-changeset --no-fail-on-empty-changeset \
  --parameter-overrides "${PARAMS:-}"
URL=$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WebhookUrl'].OutputValue" --output text)
echo "webhook URL: $URL"
inv() { aws lambda invoke --region "$REGION" --function-name posture-bot-planner \
  --cli-binary-format raw-in-base64-out --payload "$1" /dev/stdout --query StatusCode --output text; echo; }
inv '{}'
if aws ssm get-parameter --region "$REGION" --name /posture-bot/token >/dev/null 2>&1; then
  inv "{\"set_webhook\": \"$URL\"}"
else
  echo "No /posture-bot/token yet. Store it, then re-run ./deploy.sh:"
  echo "  aws ssm put-parameter --name /posture-bot/token --type SecureString --value '<token>'"
fi

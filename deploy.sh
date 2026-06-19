#!/bin/bash
#
# Deploy the exports worker (SQS -> Lambda) to Mumbai (ap-south-1).
# Idempotent: safe to re-run. Creates/updates the DLQ, queue, IAM role,
# Lambda function and event source mapping.
#
# Requires: aws cli v2, node, a populated .env (see .env.example).
#
set -euo pipefail

# ----------------------------- config -------------------------------------
REGION="ap-south-1"
PROJECT="exports-imports"
FUNCTION_NAME="exports-worker-mumbai"
ROLE_NAME="exports-imports-lambda-role-mumbai"
QUEUE_NAME="exports-queue-mumbai"
DLQ_NAME="exports-dlq-mumbai"
HANDLER="index.exportHandler"
RUNTIME="nodejs20.x"
TIMEOUT=900            # 15 min (Lambda max) — exports can be long
MEMORY=1024
VISIBILITY_TIMEOUT=960 # must exceed function timeout so SQS doesn't redeliver mid-run
MAX_RECEIVE_COUNT=3    # attempts before a message lands in the DLQ
BATCH_SIZE=1           # one heavy export per invocation
S3_EXPORT_BUCKET="ncx-prod-exports"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${DLQ_NAME}"
QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${QUEUE_NAME}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

echo "🌏 Region=${REGION}  Account=${ACCOUNT_ID}"
cd "$(dirname "$0")"

# --------------------------- 1. DLQ + queue -------------------------------
echo "📦 Ensuring DLQ ${DLQ_NAME}..."
aws sqs create-queue --queue-name "$DLQ_NAME" --region "$REGION" \
  --attributes "MessageRetentionPeriod=1209600" >/dev/null

echo "📦 Ensuring queue ${QUEUE_NAME}..."
REDRIVE="{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"${MAX_RECEIVE_COUNT}\"}"
# create-queue needs the RedrivePolicy JSON itself as a string value:
ATTRS="$(node -e "console.log(JSON.stringify({VisibilityTimeout:String($VISIBILITY_TIMEOUT),MessageRetentionPeriod:'1209600',RedrivePolicy:JSON.stringify({deadLetterTargetArn:'${DLQ_ARN}',maxReceiveCount:'${MAX_RECEIVE_COUNT}'})}))")"
QUEUE_URL="$(aws sqs create-queue --queue-name "$QUEUE_NAME" --region "$REGION" \
  --attributes "$ATTRS" --query 'QueueUrl' --output text)"
echo "   QueueUrl=${QUEUE_URL}"

# --------------------------- 2. IAM role ----------------------------------
if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "🔐 Creating IAM role ${ROLE_NAME}..."
  cat > trust-policy.json <<'EOF'
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow",
    "Principal": { "Service": "lambda.amazonaws.com" },
    "Action": "sts:AssumeRole" }] }
EOF
  aws iam create-role --role-name "$ROLE_NAME" \
    --assume-role-policy-document file://trust-policy.json >/dev/null
  aws iam attach-role-policy --role-name "$ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  rm -f trust-policy.json
  echo "⏳ waiting for role propagation..."; sleep 10
fi

echo "🔐 Putting inline policy (SQS consume + S3 export bucket)..."
cat > inline-policy.json <<EOF
{ "Version": "2012-10-17",
  "Statement": [
    { "Sid": "SqsConsume", "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:GetQueueAttributes","sqs:ChangeMessageVisibility","sqs:SendMessage"],
      "Resource": ["${QUEUE_ARN}","${DLQ_ARN}"] },
    { "Sid": "S3ExportBucket", "Effect": "Allow",
      "Action": ["s3:PutObject","s3:GetObject","s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${S3_EXPORT_BUCKET}","arn:aws:s3:::${S3_EXPORT_BUCKET}/*"] }
  ] }
EOF
aws iam put-role-policy --role-name "$ROLE_NAME" \
  --policy-name "${PROJECT}-inline" --policy-document file://inline-policy.json
rm -f inline-policy.json

# --------------------------- 3. env vars ----------------------------------
# Build the Lambda environment from .env, excluding names Lambda reserves
# (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION) — S3 uses the role.
echo "🧬 Building environment from .env..."
[ -f .env ] || { echo "❌ .env not found (copy .env.example and fill it)"; exit 1; }
EXPORT_QUEUE_URL="$QUEUE_URL" node scripts/build-env-json.js > env.json
echo "   keys: $(node -e "console.log(Object.keys(require('./env.json').Variables).join(', '))")"

# --------------------------- 4. package -----------------------------------
echo "📦 Installing production deps + zipping..."
npm install --production --no-audit --no-fund >/dev/null 2>&1
rm -f function.zip
zip -rq function.zip index.js worker/ src/ allservices/ dashboard/ node_modules/ package.json

# --------------------------- 5. function ----------------------------------
if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "📝 Updating function code + config..."
  aws lambda update-function-code --function-name "$FUNCTION_NAME" \
    --zip-file fileb://function.zip --region "$REGION" >/dev/null
  aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
  aws lambda update-function-configuration --function-name "$FUNCTION_NAME" \
    --handler "$HANDLER" --timeout "$TIMEOUT" --memory-size "$MEMORY" \
    --environment file://env.json --region "$REGION" >/dev/null
else
  echo "🆕 Creating function ${FUNCTION_NAME}..."
  aws lambda create-function --function-name "$FUNCTION_NAME" \
    --runtime "$RUNTIME" --role "$ROLE_ARN" --handler "$HANDLER" \
    --zip-file fileb://function.zip --timeout "$TIMEOUT" --memory-size "$MEMORY" \
    --environment file://env.json --region "$REGION" >/dev/null
  aws lambda wait function-active --function-name "$FUNCTION_NAME" --region "$REGION"
fi

# --------------------------- 6. event source mapping ----------------------
EXISTING_UUID="$(aws lambda list-event-source-mappings --function-name "$FUNCTION_NAME" \
  --event-source-arn "$QUEUE_ARN" --region "$REGION" \
  --query 'EventSourceMappings[0].UUID' --output text 2>/dev/null || echo "None")"
if [ "$EXISTING_UUID" = "None" ] || [ -z "$EXISTING_UUID" ]; then
  echo "🔗 Creating SQS -> Lambda event source mapping..."
  aws lambda create-event-source-mapping --function-name "$FUNCTION_NAME" \
    --event-source-arn "$QUEUE_ARN" --batch-size "$BATCH_SIZE" \
    --function-response-types ReportBatchItemFailures --region "$REGION" >/dev/null
else
  echo "🔗 Event source mapping already exists (${EXISTING_UUID})"
fi

rm -f function.zip env.json
echo "✅ Done. Send a test message with: npm run send"
aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" \
  --query 'Configuration.[FunctionName,Runtime,Handler,Timeout,MemorySize]' --output table

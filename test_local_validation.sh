#!/bin/bash
set -e

# This script tests the PR validation workflow locally
# It simulates what GitHub Actions does: only use Airflow REST API with Bearer token
# No direct Docker access (docker exec, docker cp) - just like in production

# Configuration
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8081}"
AIRFLOW_TOKEN="${AIRFLOW_TOKEN}"
PR_NUMBER="${PR_NUMBER:-1}"  # Default to PR #1
GITHUB_REPO="${GITHUB_REPO:-colav/impactu_airflow}"

if [ -z "$AIRFLOW_TOKEN" ]; then
    echo "❌ Error: AIRFLOW_TOKEN environment variable is required"
    echo "Usage: AIRFLOW_TOKEN=your_token_here ./test_local_validation.sh"
    exit 1
fi

echo "🚀 Testing PR Validation Workflow (API-only mode)"
echo "=================================================="
echo "PR: #${PR_NUMBER}"
echo "Airflow: ${AIRFLOW_URL}"
echo "Mode: REST API only (simulating GitHub Actions)"
echo ""

# Step 1: Trigger pr_validator DAG
echo "📤 Step 1: Triggering pr_validator DAG..."
LOGICAL_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
DAG_RUN_ID="pr_${PR_NUMBER}_test_$(date +%s)"

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${AIRFLOW_URL}/api/v2/dags/pr_validator/dagRuns" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${AIRFLOW_TOKEN}" \
    -d '{
        "dag_run_id": "'"${DAG_RUN_ID}"'",
        "logical_date": "'"${LOGICAL_DATE}"'",
        "conf": {
            "pr_number": '"${PR_NUMBER}"',
            "repository": "'"${GITHUB_REPO}"'"
        }
    }')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" != "200" ]; then
    echo "❌ Failed to trigger DAG (HTTP ${HTTP_CODE}):"
    echo "$BODY" | jq '.' 2>/dev/null || echo "$BODY"
    exit 1
fi

echo "  ✓ DAG triggered: ${DAG_RUN_ID}"
echo ""

# Step 2: Wait for validation results
echo "⏱️  Step 2: Polling validation status..."
MAX_ATTEMPTS=60  # 10 minutes max
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    RESPONSE=$(curl -s "${AIRFLOW_URL}/api/v2/dags/pr_validator/dagRuns/${DAG_RUN_ID}" \
        -H "Authorization: Bearer ${AIRFLOW_TOKEN}")

    STATE=$(echo "$RESPONSE" | jq -r '.state')
    DURATION=$(echo "$RESPONSE" | jq -r '.duration // "running"')

    echo "  Attempt ${ATTEMPT}/${MAX_ATTEMPTS} | Status: ${STATE} | Duration: ${DURATION}s"

    if [ "$STATE" == "success" ]; then
        echo ""
        echo "✅ Validation Passed!"
        echo ""
        echo "📊 Run Details:"
        echo "$RESPONSE" | jq '{
            dag_run_id: .dag_run_id,
            state: .state,
            start_date: .start_date,
            end_date: .end_date,
            duration: .duration,
            conf: .conf
        }'
        echo ""
        echo "🔗 View in Airflow UI:"
        echo "${AIRFLOW_URL}/dags/pr_validator/grid?dag_run_id=${DAG_RUN_ID}"
        exit 0
    elif [ "$STATE" == "failed" ]; then
        echo ""
        echo "❌ Validation Failed!"
        echo ""
        echo "📋 Fetching validation logs..."
        LOGS=$(curl -s "${AIRFLOW_URL}/api/v2/dags/pr_validator/dagRuns/${DAG_RUN_ID}/taskInstances/validate_pr/logs/1" \
            -H "Authorization: Bearer ${AIRFLOW_TOKEN}")

        echo "$LOGS" | jq -r '.content[] | select(.event) | .event' 2>/dev/null || echo "$LOGS"
        echo ""
        echo "🔗 View in Airflow UI:"
        echo "${AIRFLOW_URL}/dags/pr_validator/grid?dag_run_id=${DAG_RUN_ID}"
        exit 1
    elif [ "$STATE" == "null" ] || [ "$STATE" == "" ]; then
        echo "  ⚠️ Error fetching status"
        exit 1
    fi

    ATTEMPT=$((ATTEMPT + 1))
    sleep 10
done

echo ""
echo "⏱️ Timeout: Validation did not complete within expected time"
echo "🔗 Check status in Airflow UI:"
echo "${AIRFLOW_URL}/dags/pr_validator/grid?dag_run_id=${DAG_RUN_ID}"
exit 1

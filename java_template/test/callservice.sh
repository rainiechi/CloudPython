# JSON object to pass to Lambda Function

json='{"s3Bucket":"test.bucket.462562f23.bm", "s3Key":"data.csv"}'

echo "Invoking Lambda function using API Gateway"
time output=$(curl -s -H "Content-Type: application/json" -X POST -d "$json" https://vxawyzuez0.execute-api.us-east-2.amazonaws.com/CSVProcessor/)
echo ""

echo "JSON RESULT:"
echo "$output" | jq
echo ""
#JSON object to pass to Lambda Function
json='{"s3Bucket":"tcss462.f23.cproj", "fileName":"data.csv"}'



echo "Invoking Lambda function using API Gateway"

time output=$(curl -s -H "Content-Type: application/json" -X POST -d "$json" https://48nzswpaze.execute-api.us-east-2.amazonaws.com/python_CSVProcessor_deploy/)

echo ""



echo "JSON RESULT:"

echo "$output" | jq

echo ""
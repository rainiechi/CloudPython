#JSON object to pass to Lambda Function
json='{"s3Bucket":"tcss462.f23.cproj", "fileName":"data.csv"}'



echo "Invoking Lambda function using API Gateway"

time output=$(curl -s -H "Content-Type: application/json" -X POST -d "$json" https://68jo8ae3b1.execute-api.us-east-2.amazonaws.com/PyDataService)

echo ""



echo "JSON RESULT:"

echo "$output" | jq

echo ""
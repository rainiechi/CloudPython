# #!/bin/bash

# # JSON object to pass to Lambda Function
# json={"\"name\"":"\"Susan\u0020Smith\",\"param1\"":1,\"param2\"":2,\"param3\"":3}

# #echo "Invoking Lambda function using API Gateway"
# #time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {API-GATEWAY-REST-URL}`
# #echo ""

# #echo ""
# #echo "JSON RESULT:"
# #echo $output | jq
# #echo ""

# echo "Invoking Lambda function using AWS CLI"
# #time output=`aws lambda invoke --invocation-type RequestResponse --function-name {LAMBDA-FUNCTION-NAME} --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
# time output=`aws lambda invoke --invocation-type RequestResponse --function-name hellomysqlf21 --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

# echo ""
# echo "JSON RESULT:"
# echo $output | jq
# echo ""


csv_file_path="/home/vboxuser/CourseProject/CourseProject/100 Sales Records.csv"
api_gateway_url="your-api-gateway-url"

# Encode the CSV file to Base64
encoded_csv=$(base64 "$csv_file_path")

# Send a POST request with the encoded CSV data
curl -X POST $api_gateway_url/upload \
     -H "Content-Type: application/json" \
     -d "{\"body\":\"$encoded_csv\"}"

import json
import boto3
from botocore.exceptions import NoCredentialsError

class Helpers:
    def __init__(self):
        pass

    @staticmethod
    def s3_push(inspector, bucket_name):
        uuid_str = str(uuid.uuid4())

        # Convert Inspector HashMap to JSON
        results = inspector.finish()
        output = {str(key): str(value) for key, value in results.items()}
        json_string = json.dumps(output)

        bytes_data = json_string.encode('utf-8')
        object_key = f"run {uuid_str}.json"

        try:
            s3_client = boto3.client('s3')
            s3_client.put_object(Body=bytes_data, Bucket=bucket_name, Key=object_key, ContentType="application/json")
            print("Upload Successful")
        except NoCredentialsError as e:
            print(f"Credentials not available: {e}")
            raise e

import csv
import io
import datetime
import boto3

class Request:
    def __init__(self, s3Bucket="", s3Key="", fileName=""):
        self.s3Bucket = s3Bucket
        self.s3Key = s3Key
        self.fileName = fileName

class Response:
    def __init__(self):
        self.value = ""

class Inspector:
    def __init__(self):
        self.attributes = {}

    def finish(self):
        return self.attributes

    def inspectAll(self):
        pass

class CSVCreator:
    def handleRequest(self, request, context):
        inspector = Inspector()
        src_bucket = "test.bucket.462562f23.bm"
        src_key = "test.csv"

        try:
            csv_data = self.read_csv_from_s3(request.s3Bucket, request.s3Key)

            records_list = []
            header_added = False

            with csv.reader(io.StringIO(csv_data)) as reader:
                for record in reader:
                    if not header_added:
                        records_list.append(record)
                        header_added = True
                    else:
                        records_list.append(record)

            writer = io.StringIO()
            csv_writer = csv.writer(writer)

            for record in records_list:
                csv_writer.writerow(record)

            csv_content = writer.getvalue()

            bytes_data = csv_content.encode('utf-8')

            s3_client = boto3.client('s3')
            s3_client.put_object(Body=bytes_data, Bucket=src_bucket, Key=request.fileName)

            response = Response()
            response.value = f"Bucket:{src_bucket} filename:{request.fileName} size:{len(bytes_data)}"

            return inspector.finish()

        except Exception as e:
            print("Error uploading file to S3:", e)
            return inspector.finish()

    def read_csv_from_s3(self, bucket, key):
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_data = response['Body'].read().decode('utf-8')
        return csv_data


if __name__ == "__main__":
    context = None  # replace with the actual context if needed

    # Create an instance of the class
    csv_creator = CSVCreator()

    # Create a request object
    request = Request(s3Bucket="your_s3_bucket", s3Key="your_s3_key", fileName="output.csv")

    # Run the function
    response = csv_creator.handleRequest(request, context)

    # Print out function result
    print("Function result:", response.value)

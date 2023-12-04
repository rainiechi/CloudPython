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

    def consumeResponse(self, response):
        # Assume consumeResponse is intended to consume the response, add logic as needed
        pass

class CSVProcessor:
    def handleRequest(self, request, context):
        inspector = Inspector()

        src_bucket = "test.bucket.462562f23.bm"
        src_key = "test.csv"

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=src_bucket, Key=src_key)
        object_data = response['Body'].read().decode('utf-8')
        
        try:
            csv_reader = csv.reader(io.StringIO(object_data))
            records_list = [record for record in csv_reader]

            transform_order_priority(records_list)
            add_order_processing_time(records_list)
            add_gross_margin(records_list)
            remove_duplicate_data(records_list)

            writer = io.StringIO()
            csv_writer = csv.writer(writer)

            for record in records_list:
                csv_writer.writerow(record)

            lambda_logger = context.get_logger()
            lambda_logger.log("ProcessCSV bucketname:" + src_bucket + " filename:" + src_key + "\n")

            response = Response()
            response.value = f"Bucket:{src_bucket} filename:{request.fileName}"

            inspector.consumeResponse(response)
            return inspector.finish()

        except Exception as e:
            print("Error processing CSV:", e)
            return inspector.finish()

def add_order_processing_time(records_list):
    records_list[0].append("Processing Time")
    formatter = datetime.datetime.strptime  # TODO: Replace with the appropriate datetime formatter

    for i in range(1, len(records_list)):
        record = records_list[i]
        order_date = formatter(record[5], "%m/%d/%Y")
        ship_date = formatter(record[7], "%m/%d/%Y")
        order_processing_time = (ship_date - order_date).days
        record.append(str(order_processing_time))

def transform_order_priority(records_list):
    for i in range(1, len(records_list)):
        record = records_list[i]
        order_priority = record[4]

        if order_priority == "L":
            record[4] = "Low"
        elif order_priority == "M":
            record[4] = "Medium"
        elif order_priority == "H":
            record[4] = "High"
        elif order_priority == "C":
            record[4] = "Critical"

def add_gross_margin(records_list):
    records_list[0].append("Gross Margin")

    for i in range(1, len(records_list)):
        record = records_list[i]
        total_profit = float(record[13])
        total_revenue = float(record[11])
        gross_margin = total_profit / total_revenue
        record.append(str(gross_margin))

def remove_duplicate_data(records_list):
    processed_order_ids = set()
    i = 1

    while i < len(records_list):
        record = records_list[i]
        order_id = record[0]

        if order_id in processed_order_ids:
            records_list.pop(i)
        else:
            processed_order_ids.add(order_id)
            i += 1

if __name__ == "__main__":
    context = None  # replace with the actual context if needed

    # Create an instance of the class
    csv_processor = CSVProcessor()

    # Create a request object
    request = Request(s3Bucket="your_s3_bucket", s3Key="your_s3_key", fileName="output.csv")

    # Run the function
    response = csv_processor.handleRequest(request, context)

    # Print out function result
    print("Function result:", response.value)

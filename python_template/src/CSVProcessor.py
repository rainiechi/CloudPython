import csv
import io
import datetime
import boto3
import uuid
import Inspector
from botocore.exceptions import NoCredentialsError

class CSVProcessor:
    @staticmethod
    def add_order_processing_time(records_list):
        records_list[0].append("Processing Time")
        date_format = "%m/%d/%Y"
        
        for record in records_list[1:]:
            order_date = datetime.datetime.strptime(record[5], date_format)
            ship_date = datetime.datetime.strptime(record[7], date_format)
            order_processing_time = (ship_date - order_date).days
            record.append(str(order_processing_time))

    @staticmethod
    def transform_order_priority(records_list):
        for record in records_list[1:]:
            order_priority = record[4]

            if order_priority == "L":
                record[4] = "Low"
            elif order_priority == "M":
                record[4] = "Medium"
            elif order_priority == "H":
                record[4] = "High"
            elif order_priority == "C":
                record[4] = "Critical"


    # Add a [Gross Margin] column. The Gross Margin Column is a percentage
    # calculated using the formula: [Total Profit] / [Total Revenue].
    # It is stored as a floating point value (e.g 0.25 for 25% profit).
    @staticmethod
    def add_gross_margin(records_list):
        records_list[0].append("Gross Margin")

        for record in records_list[1:]:
            total_profit = float(record[13])
            total_revenue = float(record[11])
            gross_margin = total_profit / total_revenue
            record.append(str(gross_margin))


    # Remove duplicate data identified by [Order ID]. Any record having a duplicate
    # [Order ID] that has already been processed will be ignored.
    @staticmethod
    def remove_duplicate_data(records_list):
        processed_order_ids = set()
        index_to_remove = []

        # Skip the header row
        for i in range(1, len(records_list)):
            record = records_list[i]
            order_id = record[6]

            if order_id in processed_order_ids:
                index_to_remove.append(i)
            else:
                processed_order_ids.add(order_id)

        # Remove duplicate rows
        for index in reversed(index_to_remove):
            records_list.pop(index)


def handle_request(event, lambda_context):
    try:
        # Extract values from the event (event is the input to the Lambda function)
        src_bucket = event['src_bucket']
        src_key = event['src_key']
        filename = "transformed.csv"
        s3 = boto3.client('s3')

        response = s3.get_object(Bucket=src_bucket, Key=src_key)
        object_data = response['Body'].read().decode('utf-8')
        records_list = list(csv.reader(io.StringIO(object_data)))

        CSVProcessor.transform_order_priority(records_list)
        CSVProcessor.add_order_processing_time(records_list)
        CSVProcessor.add_gross_margin(records_list)
        CSVProcessor.remove_duplicate_data(records_list)


        # Write the transformed data to a new CSV file
        transformed_data = io.StringIO()
        csv.writer(transformed_data).writerows(records_list)

        # Upload the transformed data to S3
        s3.put_object(Body=transformed_data.getvalue(), Bucket=src_bucket, Key=filename)
        print("Successfully uploaded transformed file to S3")

        return {"Bucket": src_bucket, "Filename": filename}

    except NoCredentialsError:
        print("Credentials not available")
        return {}
import csv
import io
import datetime
import boto3
import uuid
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

    @staticmethod
    def add_gross_margin(records_list):
        records_list[0].append("Gross Margin")

        for record in records_list[1:]:
            total_profit = float(record[13])
            total_revenue = float(record[11])
            gross_margin = total_profit / total_revenue
            record.append(str(gross_margin))

    @staticmethod
    def remove_duplicate_data(records_list):
        processed_order_ids = set()
        indexes_to_remove = []

        for i, record in enumerate(records_list[1:]):
            order_id = record[0]

            if order_id in processed_order_ids:
                indexes_to_remove.append(i + 1)  # Adjust index to account for header row
            else:
                processed_order_ids.add(order_id)

        for index in sorted(indexes_to_remove, reverse=True):
            del records_list[index]

    def handle_request(self, src_bucket, src_key):
        filename = "transformed.csv"
        s3 = boto3.client('s3')

        try:
            response = s3.get_object(Bucket=src_bucket, Key=src_key)
            object_data = response['Body'].read().decode('utf-8')
            records_list = list(csv.reader(io.StringIO(object_data)))

            self.transform_order_priority(records_list)
            self.add_order_processing_time(records_list)
            self.add_gross_margin(records_list)
            self.remove_duplicate_data(records_list)

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

if __name__ == "__main__":
    processor = CSVProcessor()
    result = processor.handle_request("test.bucket.462562f23.bm", "data.csv")
    print(result)
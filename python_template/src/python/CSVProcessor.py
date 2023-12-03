import base64
import csv
import Inspector
import Helpers
from io import StringIO
from datetime import datetime
from dateutil.relativedelta import relativedelta

class CSVProcessor:
    @staticmethod
    def handle_request(event, context):
        try:
            base64_csv = event["body"]
            decoded_bytes = base64.b64decode(base64_csv)
            csv_content = decoded_bytes.decode("utf-8")

            records_list = []

            # Parse the decoded CSV content
            with StringIO(csv_content) as csv_buffer:
                reader = csv.reader(csv_buffer)
                header = next(reader)
                records_list.append(header)  # Add header first

                for row in reader:
                    records_list.append(row)

            CSVProcessor.transform_order_priority(records_list)
            CSVProcessor.add_order_processing_time(records_list)
            CSVProcessor.add_gross_margin(records_list)
            CSVProcessor.remove_duplicate_data(records_list)

            # Write the transformed data to a StringIO
            output_buffer = StringIO()
            csv_writer = csv.writer(output_buffer)
            csv_writer.writerows(records_list)

            # # Optionally convert the StringIO to bytes if needed
            # output_bytes = output_buffer.getvalue().encode('utf-8')

            # Create an instance of Inspector (or modify the s3_push method to directly accept bytes)
            inspector = Inspector()
            inspector.add_attribute("CSVContent", csv_content)

            # Upload to S3 (assuming Helpers.s3_push can handle bytes)
            bucket_name = "<your-s3-bucket-name>"
            Helpers.s3_push(inspector, bucket_name)

            return "CSV file processed and uploaded successfully"

        except Exception as e:
            print(e)
            return f"Error processing CSV file: {str(e)}"

    @staticmethod
    def add_order_processing_time(records_list):
        records_list[0].append("Processing Time")
        date_format = "%m/%d/%Y"

        # Skip the header row
        for i in range(1, len(records_list)):
            record = records_list[i]
            order_date = datetime.strptime(record[5], date_format)
            ship_date = datetime.strptime(record[7], date_format)
            order_processing_time = (ship_date - order_date).days
            record.append(str(order_processing_time))

    @staticmethod
    def transform_order_priority(records_list):
        # Skip the header row
        for i in range(1, len(records_list)):
            record = records_list[i]
            order_priority = record[4]

            priority_mapping = {"L": "Low", "M": "Medium", "H": "High", "C": "Critical"}
            record[4] = priority_mapping.get(order_priority, order_priority)

    @staticmethod
    def add_gross_margin(records_list):
        records_list[0].append("Gross Margin")

        # Skip the header row
        for i in range(1, len(records_list)):
            record = records_list[i]
            total_profit = float(record[13])
            total_revenue = float(record[11])

            if total_revenue != 0:
                gross_margin = total_profit / total_revenue
                record.append(str(gross_margin))
            else:
                record.append("0.0")

    @staticmethod
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

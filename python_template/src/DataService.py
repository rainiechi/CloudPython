import csv
import io
import os
import boto3
import logging

import mysql.connector
from botocore.exceptions import NoCredentialsError
from Inspector import Inspector

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handle_request(request, context):
        inspector = Inspector()

        
        # Create logger
        logger.info("Adding table to database")
        logger.info(f"request: bucket: {request['s3Bucket']} key: {request['s3Key']}")

        # setCurrentDirectory("/tmp")
        logger.info("reading file from s3")

        s3_client = boto3.client('s3')
        try:
            s3_object = s3_client.get_object(Bucket=request['s3Bucket'], Key=request['s3Key'])
  
            object_data = s3_object['Body'].read()

            logger.info("Data now in input stream")

            try:
                csv_data = object_data.decode("utf-8")
                
                # Establish a database connection
                conn = mysql.connector.connect(
                    host='18.220.158.6',
                    user='admin',
                    password='changeme',
                    database='python-sales-data-instance-1'
                )

                logger.info("Connection established successfully")
                cursor = conn.cursor()

                # Drop table if exists
                cursor.execute("DROP TABLE IF EXISTS Sales;")

                # Create table
                cursor.execute("""
                    CREATE TABLE Sales (
                        Region VARCHAR(100),
                        Country VARCHAR(32),
                        Item_Type VARCHAR(32),
                        Sales_Channel VARCHAR(32),
                        Order_Priority VARCHAR(32),
                        Order_Date VARCHAR(32),
                        Order_ID INT,
                        Ship_Date VARCHAR(32),
                        Units_Sold INT,
                        Unit_Price DECIMAL(10, 2),
                        Unit_Cost DECIMAL(10, 2),
                        Total_Revenue DECIMAL(10, 2),
                        Total_Cost DECIMAL(10, 2),
                        Total_Profit DECIMAL(10, 2),
                        Processing_Time INT,
                        Gross_Margin DECIMAL(10, 4)
                    );
                """)

                logger.info("Table created successfully")
                records_list = []

                logger.info("Adding data from CSV file to database")
                # Read the CSV file and store the records in a list
                csv_reader = csv.reader(io.StringIO(csv_data))
                header = next(csv_reader)
                records_list.append(header)  # Add header

                try:
                    # Insert data into the table
                    cursor.executemany("""
                        INSERT INTO Sales VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, csv_reader)

                except mysql.connector.Error as e:
                    logger.info("Got an exception working with MySQL! ")
                    logger.info(e.msg)
                    raise

                conn.commit()
                conn.close()

            except Exception as e:
                logger.info("Got an exception working with MySQL! ")
                logger.info(str(e))

        except NoCredentialsError:
            logger.info("Credentials not available")
        inspector.inspectAllDeltas()
        return inspector.finish()

if __name__ == "__main__":
    # Replace with your actual request and context objects
    sample_request = {'s3Bucket': 'your_bucket', 's3Key': 'your_key'}
    sample_context = None  # You would need to mock or create a context object for local testing.

    # Call the handle_request function directly for testing
    print(handle_request(sample_request, sample_context))
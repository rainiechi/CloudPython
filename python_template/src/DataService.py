import csv
import io
import os
import boto3
import mysql.connector
from botocore.exceptions import NoCredentialsError
from Inspector import Inspector

class DataService:

    CONTAINER_ID = "/tmp/container-id"
    CHARSET = "US-ASCII"

    @staticmethod
    def handle_request(request, context):
        inspector = Inspector()
        logger = context.getLogger()
        
        # Create logger
        logger.log("Adding table to database")
        logger.log(f"request: bucket: {request['s3Bucket']} key: {request['s3Key']}")

        # setCurrentDirectory("/tmp")
        logger.log("reading file from s3")

        s3_client = boto3.client('s3')
        try:
            s3_object = s3_client.get_object(Bucket=request['s3Bucket'], Key=request['s3Key'])
            logger.log("successfully read csv file from s3")
            object_data = s3_object['Body'].read()

            logger.log("Data now in input stream")

            try:
                csv_data = object_data.decode("utf-8")
                
                # Replace properties file with actual database connection details
                # properties = {'host': 'your_database_host', 'user': 'your_username', 'password': 'your_password', 'database': 'your_database_name'}
                # Replace the following four lines with your actual database connection details
                # host = properties['host']
                # user = properties['user']
                # password = properties['password']
                # database = properties['database']
                
                # Establish a database connection
                conn = mysql.connector.connect(
                    host='18.220.158.6',
                    user='admin',
                    password='changeme',
                    database='python-sales-data-instance-1'
                )

                logger.log("Connection established successfully")
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

                logger.log("Table created successfully")
                records_list = []

                logger.log("Adding data from CSV file to database")
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
                    logger.log("Got an exception working with MySQL! ")
                    logger.log(e.msg)
                    raise

                conn.commit()
                conn.close()

            except Exception as e:
                logger.log("Got an exception working with MySQL! ")
                logger.log(str(e))

        except NoCredentialsError:
            logger.log("Credentials not available")
        inspector.inspectAllDeltas()
        return inspector.finish()

if __name__ == "__main__":
    # Replace with your actual request and context objects
    sample_request = {'s3Bucket': 'your_bucket', 's3Key': 'your_key'}
    sample_context = None

    # Instantiate the class and call the handle_request method
    data_service = DataService()
    data_service.handle_request(sample_request, sample_context)
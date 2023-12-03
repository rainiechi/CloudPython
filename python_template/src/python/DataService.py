import csv
import os
import pymysql
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

class Request:
    def __init__(self, name=""):
        self.fileName = name

class Response:
    pass

class Context:
    def getLogger(self):
        return LambdaLogger()

class LambdaLogger:
    def log(self, string):
        print(f"LOG: {string}")

class Inspector:
    def __init__(self):
        self.attributes = {}

    def inspectAll(self):
        pass

    def inspectAllDeltas(self):
        pass

    def addAttribute(self, key, value):
        self.attributes[key] = value

    def finish(self):
        return self.attributes

class DataService:
    def handleRequest(self, request, context):
        # Create logger
        logger = context.getLogger()
        inspector = Inspector()
        inspector.inspectAll()

        csv_data = self.read_csv_from_s3(request.s3Bucket, request.s3Key)

        try:
            db_properties = {
                "url": "your_database_url",
                "username": "your_database_username",
                "password": "your_database_password",
                "driver": "com.mysql.cj.jdbc.Driver"
            }

            con = pymysql.connect(
                host=db_properties["url"],
                user=db_properties["username"],
                password=db_properties["password"]
            )

            cursor = con.cursor()

            cursor.execute("DROP TABLE IF EXISTS mytable;")
            cursor.execute("""
                CREATE TABLE mytable (
                    Region VARCHAR(32),
                    Country VARCHAR(32),
                    Item_Type VARCHAR(32),
                    Sales_Channel VARCHAR(32),
                    Order_Priority CHAR(1),
                    Order_Date DATE,
                    Order_ID INT,
                    Ship_Date DATE,
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

            records_list = []

            with csv.reader(csv_data.splitlines()) as reader:
                header = next(reader)
                records_list.append(header)

                for row in reader:
                    records_list.append(row)
                    cursor.execute("""
                        INSERT INTO mytable VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, tuple(row))

            con.commit()
            con.close()

        except Exception as e:
            logger.log(f"Got an exception working with MySQL! {e}")

        inspector.inspectAllDeltas()
        return inspector.finish()

    def read_csv_from_s3(self, bucket, key):
        s3_client = boto3.client("s3")
        try:
            s3_object = s3_client.get_object(Bucket=bucket, Key=key)
            csv_data = s3_object["Body"].read().decode("utf-8")
            return csv_data
        except NoCredentialsError as e:
            raise Exception("Credentials not available") from e

if __name__ == "__main__":
    context = Context()

    # Create an instance of the class
    data_service = DataService()

    # Create a request object
    request = Request()

    # Grab the name from the cmdline from arg 0
    name = input("Enter the S3 Key: ")

    # Load the name into the request object
    request.s3Key = name

    # Run the function
    resp = data_service.handleRequest(request, context)

    # Print out function result
    print("function result:", resp)

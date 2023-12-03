package lambda;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import saaf.Inspector;

import javax.sql.DataSource;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

public class DataService implements RequestHandler<Request, HashMap<String, Object>>
{


    //Creating a relational database for CSV file with columns: Region	Country	Item Type	Sales Channel	Order Priority	Order Date	Order ID	Ship Date	Units Sold	Unit Price	Unit Cost	Total Revenue	Total Cost	Total Profit	Processing Time	Gross Margin
    public HashMap<String, Object> handleRequest(Request request, Context context)
    {

        // Create logger
        LambdaLogger logger = context.getLogger();
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        String csvData = readCsvFromS3(request.getS3Bucket(), request.getS3Key());


        try
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");


            Connection con = DriverManager.getConnection(url, username, password);

            PreparedStatement ps = con.prepareStatement("drop table if exists mytable;");

            ps.execute();
            ps = con.prepareStatement(
                    "CREATE TABLE mytable (" +
                            "Region VARCHAR(32), " +
                            "Country VARCHAR(32), " +
                            "Item_Type VARCHAR(32), " +
                            "Sales_Channel VARCHAR(32), " +
                            "Order_Priority CHAR(1), " +
                            "Order_Date DATE, " +
                            "Order_ID INT, " +
                            "Ship_Date DATE, " +
                            "Units_Sold INT, " +
                            "Unit_Price DECIMAL(10, 2), " +
                            "Unit_Cost DECIMAL(10, 2), " +
                            "Total_Revenue DECIMAL(10, 2), " +
                            "Total_Cost DECIMAL(10, 2), " +
                            "Total_Profit DECIMAL(10, 2), " +
                            "Processing_Time INT, " +
                            "Gross_Margin DECIMAL(10, 4)" +
                            ");"
                                     );
            ps.execute();

            List<List<String>> recordsList = new ArrayList<>();

            // Read the CSV file and store the records in a list
            try (CSVParser parser = new CSVParser(new FileReader(request.getFileName()), CSVFormat.DEFAULT.withFirstRecordAsHeader()))
            {
                recordsList.add(new ArrayList<>(parser.getHeaderNames()));  // Add header first
                try
                {
                    ps = con.prepareStatement("INSERT INTO mytable VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");


                    for (CSVRecord record : parser)
                    {
                        List<String> newRecord = new ArrayList<>();
                        record.forEach(newRecord::add);
                        recordsList.add(newRecord);
                        ps.setString(1, newRecord.get(0));                       // Region
                        ps.setString(2, newRecord.get(1));                       // Country
                        ps.setString(3, newRecord.get(2));                       // Item Type
                        ps.setString(4, newRecord.get(3));                       // Sales Channel
                        ps.setString(5, newRecord.get(4));                       // Order Priority
                        ps.setDate(6, java.sql.Date.valueOf(newRecord.get(5)));  // Order Date
                        ps.setInt(7, Integer.parseInt(newRecord.get(6)));        // Order ID
                        ps.setDate(8, java.sql.Date.valueOf(newRecord.get(7)));  // Ship Date
                        ps.setInt(9, Integer.parseInt(newRecord.get(8)));        // Units Sold
                        ps.setDouble(10, Double.parseDouble(newRecord.get(9)));  // Unit Price
                        ps.setDouble(11, Double.parseDouble(newRecord.get(10))); // Unit Cost
                        ps.setDouble(12, Double.parseDouble(newRecord.get(11))); // Total Revenue
                        ps.setDouble(13, Double.parseDouble(newRecord.get(12))); // Total Cost
                        ps.setDouble(14, Double.parseDouble(newRecord.get(13))); // Total Profit
                        ps.setInt(15, Integer.parseInt(newRecord.get(14)));      // Processing Time
                        ps.setDouble(16, Double.parseDouble(newRecord.get(15))); // Gross Margin
                        ps.addBatch();
                    }
                    ps.executeBatch();

                } catch (SQLException theE)
                {
                    throw new RuntimeException(theE);
                }
            }


            con.close();
//            r.setNames(ll);
        } catch (
                Exception e)

        {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }

        //Print log information to the Lambda log as needed
        //logger.log("log message...");



        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }


    // int main enables testing function from cmd line
    public static void main(String[] args)
    {
        Context c = new Context()
        {
            @Override
            public String getAwsRequestId()
            {
                return "";
            }

            @Override
            public String getLogGroupName()
            {
                return "";
            }

            @Override
            public String getLogStreamName()
            {
                return "";
            }

            @Override
            public String getFunctionName()
            {
                return "";
            }

            @Override
            public String getFunctionVersion()
            {
                return "";
            }

            @Override
            public String getInvokedFunctionArn()
            {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity()
            {
                return null;
            }

            @Override
            public ClientContext getClientContext()
            {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis()
            {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB()
            {
                return 0;
            }

            @Override
            public LambdaLogger getLogger()
            {
                return new LambdaLogger()
                {
                    @Override
                    public void log(String string)
                    {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };




        // Create an instance of the class
        DataService lt = new DataService();

        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        req.setName(name);

        // Report name to stdout
        System.out.println("cmd-line param name=" + req.getFileName());

        // Test properties file creation
        Properties properties = new Properties();
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("url", "");
        properties.setProperty("username", "");
        properties.setProperty("password", "");
        try
        {
            properties.store(new FileOutputStream("test.properties"), "");
        } catch (IOException ioe)
        {
            System.out.println("error creating properties file.");
        }


        // Run the function
        //Response resp = lt.handleRequest(req, c);
        System.out.println("The MySQL Serverless can't be called directly without running on the same VPC as the RDS cluster.");
        Response resp = new Response();

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }


    private String readCsvFromS3(String bucket, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8))) {
            
            StringBuilder csvData = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                csvData.append(line).append("\n");
            }
            return csvData.toString();
        } catch (IOException e) {
            // Handle exceptions
            throw new RuntimeException("Error reading CSV from S3", e);
        }
    }

}

package lambda;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class DataService implements RequestHandler<Request, HashMap<String, Object>> {

    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");

    // Creating a relational database for CSV file with columns: Region Country Item
    // Type Sales Channel Order Priority Order Date Order ID Ship Date Units Sold
    // Unit Price Unit Cost Total Revenue Total Cost Total Profit Processing Time
    // Gross Margin
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        Inspector inspector = new Inspector();

        // Create logger
        logger.log("Adding table to database");
        logger.log("request: bucket: " + request.getS3Bucket() + " key: " + request.getS3Key());

        // setCurrentDirectory("/tmp");
        logger.log("reading file from s3");

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                request.getS3Bucket(), request.getS3Key()));
        logger.log("successfully read csv file from s3");
        InputStream objectData = s3Object.getObjectContent();

        logger.log("Data now in input stream");

        try {

            Scanner scanner = new Scanner(objectData);
            StringBuilder stringBuilder = new StringBuilder();
            while (scanner.hasNextLine()) {
                stringBuilder.append(scanner.nextLine());
                stringBuilder.append("\n");
            }

            String csvData = stringBuilder.toString();


            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");

            logger.log("DATA: " + csvData);

            // Connection con = DriverManager.getConnection("jdbc:sqlite:");
            Connection con = DriverManager.getConnection(url, username, password);

            logger.log("Connection established successfully");
            PreparedStatement ps = con.prepareStatement("drop table if exists Sales;");

            ps.execute();
            ps = con.prepareStatement(
                    "CREATE TABLE Sales (" +
                            "Region VARCHAR(100), " +
                            "Country VARCHAR(32), " +
                            "Item_Type VARCHAR(32), " +
                            "Sales_Channel VARCHAR(32), " +
                            "Order_Priority VARCHAR(32), " +
                            "Order_Date VARCHAR(32), " +
                            "Order_ID INT, " +
                            "Ship_Date VARCHAR(32), " +
                            "Units_Sold INT, " +
                            "Unit_Price DECIMAL(10, 2), " +
                            "Unit_Cost DECIMAL(10, 2), " +
                            "Total_Revenue DECIMAL(10, 2), " +
                            "Total_Cost DECIMAL(10, 2), " +
                            "Total_Profit DECIMAL(10, 2), " +
                            "Processing_Time INT, " +
                            "Gross_Margin DECIMAL(10, 4)" +
                            ");");
            ps.execute();

            logger.log("Table created successfully");
            List<List<String>> recordsList = new ArrayList<>();

            logger.log("Adding data from CSV file to database");
            // Read the CSV file and store the records in a list
            try (CSVParser parser = new CSVParser(new StringReader(csvData),
                    CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                recordsList.add(new ArrayList<>(parser.getHeaderNames())); // Add header

                try {
                    ps = con.prepareStatement("INSERT INTO Sales VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

                    for (CSVRecord record : parser) {
                        List<String> newRecord = new ArrayList<>();
                        record.forEach(newRecord::add);
                        recordsList.add(newRecord);
                        ps.setString(1, newRecord.get(0)); // Region
                        ps.setString(2, newRecord.get(1)); // Country
                        ps.setString(3, newRecord.get(2)); // Item Type
                        ps.setString(4, newRecord.get(3)); // Sales Channel
                        ps.setString(5, newRecord.get(4)); // Order Priority
                        ps.setString(6, (newRecord.get(5))); // Order Date
                        ps.setInt(7, Integer.parseInt(newRecord.get(6))); // Order ID
                        ps.setString(8, (newRecord.get(7))); // Ship Date
                        ps.setInt(9, Integer.parseInt(newRecord.get(8))); // Units Sold
                        ps.setDouble(10, Double.parseDouble(newRecord.get(9))); // Unit Price
                        ps.setDouble(11, Double.parseDouble(newRecord.get(10))); // Unit Cost
                        ps.setDouble(12, Double.parseDouble(newRecord.get(11))); // Total Revenue
                        ps.setDouble(13, Double.parseDouble(newRecord.get(12))); // Total Cost
                        ps.setDouble(14, Double.parseDouble(newRecord.get(13))); // Total Profit
                        ps.setInt(15, Integer.parseInt(newRecord.get(14))); // Processing Time
                        ps.setDouble(16, Double.parseDouble(newRecord.get(15))); // Gross Margin
                        ps.addBatch();
                    }
                    ps.executeBatch();

                } catch (SQLException theE) {
                    logger.log("Got an exception working with MySQL! ");
                    logger.log(theE.getMessage());
                    throw new RuntimeException(theE);
                }
            }

            con.close();
            // r.setNames(ll);
        } catch (Exception e)

        {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

}
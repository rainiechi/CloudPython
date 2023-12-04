package lambda;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.System.Logger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import saaf.Inspector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.IOUtils;

// //imorts for aws s3

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/*
Example Service #1 transformations (can implement others):
 1. Add column [Order Processing Time] column that stores an integer value representing the number of days between the [Order Date] and [Ship Date]

 2. Transform [Order Priority] column: L to “Low” M to “Medium” H to “High” C to “Critical”

 3. Add a [Gross Margin] column. The Gross Margin Column is a percentage calculated using the formula: [Total Profit] /  [Total Revenue].
 It is stored as a floating point value (e.g 0.25 for 25% profit).

 4. Remove duplicate data identified by [Order ID]. Any record having a duplicate [Order ID] that has already been processed will be ignored.
 */

public class CSVCreator implements RequestHandler<Request, HashMap<String, Object>> {

    public HashMap<String, Object> handleRequest(Request request, Context context) {

        String filename = request.getFileName();
        Inspector inspector = new Inspector();

        String srcBucket = "test.bucket.462562f23.bm";
        String srcKey = "test.csv";

        Logger logger = System.getLogger("CSVProcessor");

        try {

            // // Read CSV data from S3
            String csvData = readCsvFromS3(request.getS3Bucket(), request.getS3Key());

            List<List<String>> recordsList = new ArrayList<>();

            // Parse the decoded CSV content
            try (CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new StringReader(csvContent))) {
                recordsList.add(new ArrayList<>(parser.getHeaderNames())); // Add header first

                for (CSVRecord record : parser) {
                    List<String> newRecord = new ArrayList<>();
                    record.forEach(newRecord::add);
                    recordsList.add(newRecord);
                }
            }

            // Write the transformed data to a StringWriter
            StringWriter writer = new StringWriter();
            try (CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
                for (List<String> record : recordsList) {
                    printer.printRecord(record);
                }
            }

            // // Optionally convert the StringWriter to InputStream if needed
            byte[] bytes = writer.toString().getBytes(StandardCharsets.UTF_8);
            InputStream is = new ByteArrayInputStream(bytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/plain"); // Create new file on S3
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            s3Client.putObject(srcBucket, filename, is, meta);
            Response response = new Response();
            System.out.println("Succesfully uploaded file to S3");
            response.setValue("Bucket:" + srcBucket + " filename:" + filename + " size:" + bytes.length);

            return inspector.finish();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error uploading file to S3");
            return inspector.finish();

        }
    }

    private String readCsvFromS3(String bucket, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8))) {

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

    private static void addOrderProcessingTime(List<List<String>> recordsList) {
        recordsList.get(0).add("Processing Time");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");

        // Skip the header row
        for (int i = 1; i < recordsList.size(); i++) {
            List<String> record = recordsList.get(i);

            LocalDate orderDate = LocalDate.parse(record.get(5), formatter);
            LocalDate shipDate = LocalDate.parse(record.get(7), formatter);
            long orderProcessingTime = java.time.temporal.ChronoUnit.DAYS.between(orderDate, shipDate);

            record.add(String.valueOf(orderProcessingTime));
        }
    }

    private static void transformOrderPriority(List<List<String>> recordsList) {
        // Skip the header row
        for (int i = 1; i < recordsList.size(); i++) {
            List<String> record = recordsList.get(i);
            String orderPriority = record.get(4);

            switch (orderPriority) {
                case "L":
                    orderPriority = "Low";
                    break;
                case "M":
                    orderPriority = "Medium";
                    break;
                case "H":
                    orderPriority = "High";
                    break;
                case "C":
                    orderPriority = "Critical";
                    break;
            }
            record.set(4, orderPriority); // Modify the existing Order Priority column
        }
    }

    // Add a [Gross Margin] column. The Gross Margin Column is a percentage
    // calculated using the formula: [Total Profit] / [Total Revenue].
    // It is stored as a floating point value (e.g 0.25 for 25% profit).
    private static void addGrossMargin(List<List<String>> recordsList) {
        recordsList.get(0).add("Gross Margin");
        // Skip the header row
        for (int i = 1; i < recordsList.size(); i++) {
            List<String> record = recordsList.get(i);

            float totalProfit = Float.parseFloat(record.get(13));
            float totalRevenue = Float.parseFloat(record.get(11));
            float grossMargin = totalProfit / totalRevenue;

            record.add(String.valueOf(grossMargin)); // Add the new column
        }
    }

    // Remove duplicate data identified by [Order ID]. Any record having a duplicate
    // [Order ID] that has already been processed will be ignored.
    private static void removeDuplicateData(List<List<String>> recordsList) {
        ArrayList<String> processedOrderIds = new ArrayList<>();

        // Skip the header row
        for (int i = 1; i < recordsList.size(); i++) {
            List<String> record = recordsList.get(i);
            String orderId = record.get(0);
            if (processedOrderIds.contains(orderId)) {
                recordsList.remove(i);
            } else {
                processedOrderIds.add(orderId);
            }
        }
    }
}
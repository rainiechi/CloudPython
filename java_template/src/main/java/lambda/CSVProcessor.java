package lambda;


import java.io.ByteArrayInputStream;
import java.io.InputStream;


import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;

import org.apache.commons.csv.CSVPrinter;


import saaf.Inspector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

// //imorts for aws s3

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.util.HashMap;
import java.util.Scanner;

/*
Example Service #1 transformations (can implement others):
 1. Add column [Order Processing Time] column that stores an integer value representing the number of days between the [Order Date] and [Ship Date]

 2. Transform [Order Priority] column: L to “Low” M to “Medium” H to “High” C to “Critical”

 3. Add a [Gross Margin] column. The Gross Margin Column is a percentage calculated using the formula: [Total Profit] /  [Total Revenue].
 It is stored as a floating point value (e.g 0.25 for 25% profit).

 4. Remove duplicate data identified by [Order ID]. Any record having a duplicate [Order ID] that has already been processed will be ignored.
 */

public class CSVProcessor implements RequestHandler<Request, HashMap<String, Object>> {

    public HashMap<String, Object> handleRequest(Request request, Context context) {
        String filename = "transformed.csv";
        Inspector inspector = new Inspector();
        // inspector.inspectAll();

        String srcBucket = "test.bucket.462562f23.bm";
        String srcKey = "data.csv";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                srcBucket, srcKey));


        InputStream objectData = s3Object.getObjectContent();

        try {

            Scanner scanner = new Scanner(objectData);

            // // Read CSV data from S3
            List<List<String>> recordsList = new ArrayList<>();

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] values = line.split(",");
                List<String> record = new ArrayList<>();
                for (String value : values) {
                    record.add(value);
                }
                recordsList.add(record);
            }

            transformOrderPriority(recordsList);
            addOrderProcessingTime(recordsList);
            addGrossMargin(recordsList);
            removeDuplicateData(recordsList);

            // Write the transformed data to a StringWriter
            StringWriter writer = new StringWriter();
            try (CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
                for (List<String> record : recordsList) {
                    printer.printRecord(record);
                }
            }

            scanner.close();
            LambdaLogger logger = context.getLogger();
            logger.log("ProcessCSV bucketname:" + srcBucket + " filename:" + srcKey + "\n");

            try {

                byte[] bytes = writer.toString().getBytes(StandardCharsets.UTF_8);
                InputStream is = new ByteArrayInputStream(bytes);
                ObjectMetadata meta = new ObjectMetadata();
                meta.setContentLength(bytes.length);
                meta.setContentType("text/plain"); // Create new file on S3

                s3Client.putObject(srcBucket, filename, is, meta);
                System.out.println("Succesfully uploaded transformed file to S3");

                          Response response = new Response();
            response.setValue("Bucket:" + srcBucket + " filename:" + filename);

            // inspector.consumeResponse(response);
                return inspector.finish();

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error uploading file to S3");
                return inspector.finish();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error reading CSV from S3");
            return inspector.finish();
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
            String orderId = record.get(6);
            if (processedOrderIds.contains(orderId)) {
                recordsList.remove(i);
            } else {
                processedOrderIds.add(orderId);
            }
        }
    }
}
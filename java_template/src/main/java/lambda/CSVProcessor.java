package lambda;

import java.io.StringReader;
import java.io.StringWriter;

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
import java.util.Base64;
import java.util.Map;

/*
Example Service #1 transformations (can implement others):
 1. Add column [Order Processing Time] column that stores an integer value representing the number of days between the [Order Date] and [Ship Date]

 2. Transform [Order Priority] column: L to “Low” M to “Medium” H to “High” C to “Critical”

 3. Add a [Gross Margin] column. The Gross Margin Column is a percentage calculated using the formula: [Total Profit] /  [Total Revenue].
 It is stored as a floating point value (e.g 0.25 for 25% profit).

 4. Remove duplicate data identified by [Order ID]. Any record having a duplicate [Order ID] that has already been processed will be ignored.
 */

public class CSVProcessor implements RequestHandler<Map<String, Object>, String> {

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        try {
            String base64Csv = (String) event.get("body");
            byte[] decodedBytes = Base64.getDecoder().decode(base64Csv);
            String csvContent = new String(decodedBytes);

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

            // // Optionally convert the StringWriter to InputStream if needed
            // InputStream inputStream = IOUtils.toInputStream(writer.toString(),
            // StandardCharsets.UTF_8);

            // Create an instance of Inspector (or modify the s3Push method to directly
            // accept InputStream)
            Inspector inspector = new Inspector();

            inspector.addAttribute("CSVContent", csvContent);


            // Upload to S3 (assuming Helpers.s3Push can handle InputStream)
            String bucketName = "<your-s3-bucket-name>";
            Helpers.s3Push(inspector, bucketName);

            return "CSV file processed and uploaded successfully";

        } catch (Exception e) {
            e.printStackTrace();
            return "Error processing CSV file: " + e.getMessage();
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

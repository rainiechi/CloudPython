// package lambda;

// //imorts for aws s3
// import com.amazonaws.services.s3.AmazonS3;
// import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import com.amazonaws.services.s3.model.GetObjectRequest;
// import com.amazonaws.services.s3.model.S3Object;

// import com.amazonaws.services.lambda.runtime.ClientContext;
// import com.amazonaws.services.lambda.runtime.CognitoIdentity;
// import com.amazonaws.services.lambda.runtime.Context; 
// import com.amazonaws.services.lambda.runtime.RequestHandler;
// import com.amazonaws.services.lambda.runtime.LambdaLogger;

// import java.io.BufferedReader;
// import java.io.FileInputStream;
// import java.io.FileOutputStream;
// import java.io.IOException;
// import java.io.InputStreamReader;
// import java.nio.charset.Charset;
// import java.nio.charset.StandardCharsets;
// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.PreparedStatement;
// import java.sql.ResultSet;
// import java.util.LinkedList;
// import java.util.Properties;
// import saaf.Inspector;
// import java.util.HashMap;

// /**
//  * uwt.lambda_test::handleRequest
//  *
//  * @author Wes Lloyd
//  * @author Robert Cordingly
//  */
// public class HelloMySQL implements RequestHandler<Request, HashMap<String, Object>> {

//     /**
//      * Lambda Function Handler
//      * 
//      * @param request Request POJO with defined variables from Request.java
//      * @param context 
//      * @return HashMap that Lambda will automatically convert into JSON.
//      */
//     public HashMap<String, Object> handleRequest(Request request, Context context) {

//         // Create logger
//         LambdaLogger logger = context.getLogger();        

//         //Collect inital data.
//         Inspector inspector = new Inspector();
//         inspector.inspectAll();
        
//         //****************START FUNCTION IMPLEMENTATION*************************
//         //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        

//         try 
//         {
//             Properties properties = new Properties();
//             properties.load(new FileInputStream("db.properties"));
            
//             String url = properties.getProperty("url");
//             String username = properties.getProperty("username");
//             String password = properties.getProperty("password");
//             String driver = properties.getProperty("driver");
            

//             // Read CSV data from S3
//               String csvData = readCsvFromS3(request.getS3Bucket(), request.getS3Key());


//             // Manually loading the JDBC Driver is commented out
//             // No longer required since JDBC 4
//             //Class.forName(driver);
//             Connection con = DriverManager.getConnection(url,username,password);
            
//             // PreparedStatement ps = con.prepareStatement("insert into mytable values('" + request.getName() + "','b','c');");
//             ps.execute();
//             ps = con.prepareStatement("select * from mytable;");
//             ResultSet rs = ps.executeQuery();
//             LinkedList<String> ll = new LinkedList<String>();
//             while (rs.next())
//             {
//                 logger.log("name=" + rs.getString("name"));
//                 ll.add(rs.getString("name"));
//                 logger.log("col2=" + rs.getString("col2"));
//                 logger.log("col3=" + rs.getString("col3"));
//             }
//             rs.close();
//             con.close();

//         } 
//         catch (Exception e) 
//         {
//             logger.log("Got an exception working with MySQL! ");
//             logger.log(e.getMessage());
//         }

//         //Print log information to the Lambda log as needed
//         //logger.log("log message...");
        
        
//         //****************END FUNCTION IMPLEMENTATION***************************
        
//         //Collect final information such as total runtime and cpu deltas.
//         inspector.inspectAllDeltas();
//         return inspector.finish();
//     }


//         private String readCsvFromS3(String bucket, String key) {
//         AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
//         try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
//              BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8))) {
            
//             StringBuilder csvData = new StringBuilder();
//             String line;
//             while ((line = reader.readLine()) != null) {
//                 csvData.append(line).append("\n");
//             }
//             return csvData.toString();
//         } catch (IOException e) {
//             // Handle exceptions
//             throw new RuntimeException("Error reading CSV from S3", e);
//         }
//         }


//     // int main enables testing function from cmd line
//     public static void main (String[] args)
//     {
//         Context c = new Context() {
//             @Override
//             public String getAwsRequestId() {
//                 return "";
//             }

//             @Override
//             public String getLogGroupName() {
//                 return "";
//             }

//             @Override
//             public String getLogStreamName() {
//                 return "";
//             }

//             @Override
//             public String getFunctionName() {
//                 return "";
//             }

//             @Override
//             public String getFunctionVersion() {
//                 return "";
//             }

//             @Override
//             public String getInvokedFunctionArn() {
//                 return "";
//             }

//             @Override
//             public CognitoIdentity getIdentity() {
//                 return null;
//             }

//             @Override
//             public ClientContext getClientContext() {
//                 return null;
//             }

//             @Override
//             public int getRemainingTimeInMillis() {
//                 return 0;
//             }

//             @Override
//             public int getMemoryLimitInMB() {
//                 return 0;
//             }

//             @Override
//             public LambdaLogger getLogger() {
//                 return new LambdaLogger() {
//                     @Override
//                     public void log(String string) {
//                         System.out.println("LOG:" + string);
//                     }
//                 };
//             }
//         };

//         // Create an instance of the class
//         HelloMySQL lt = new HelloMySQL();

//         // Create a request object
//         Request req = new Request();

//         // Grab the name from the cmdline from arg 0
//         String name = (args.length > 0 ? args[0] : "");

//         // Load the name into the request object
//         req.setName(name);

      
//         // Test properties file creation
//         Properties properties = new Properties();
//         properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
//         properties.setProperty("url","");
//         properties.setProperty("username","");
//         properties.setProperty("password","");
//         try
//         {
//           properties.store(new FileOutputStream("test.properties"),"");
//         }
//         catch (IOException ioe)
//         {
//           System.out.println("error creating properties file.")   ;
//         }


//         // Run the function
//         //Response resp = lt.handleRequest(req, c);
//         System.out.println("The MySQL Serverless can't be called directly without running on the same VPC as the RDS cluster.");
//         Response resp = new Response();

//         // Print out function result
//         System.out.println("function result:" + resp.toString());
//     }

// }

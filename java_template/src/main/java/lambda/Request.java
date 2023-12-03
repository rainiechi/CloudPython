package lambda;

import java.io.File;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String fileName;
    private String s3Bucket;
    private String s3Key;


    public String getFileName() {
        return fileName;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public String getS3Key() {
        return s3Key;
    }

    public void setName(String fileName) {
        this.fileName = fileName;
    }

    public Request(String fileName) {
        this.fileName = fileName;
    }

    public Request() {

    }
}

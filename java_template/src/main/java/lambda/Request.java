package lambda;

import java.io.File;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String fileName;

    public String getFileName() {
        return fileName;
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

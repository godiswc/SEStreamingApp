import com.onecloud.tsdb.demo.TSDBDataRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


/**
 * Created by root on 5/31/17.
 */
public class TestDataRetriever {

    private Logger logger = LoggerFactory.getLogger(TestDataRetriever.class);

    public static void main(String[] args){
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("./conf/producer.properties"));
            TSDBDataRetriever retriever = new TSDBDataRetriever();
            retriever.retrieve(prop);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

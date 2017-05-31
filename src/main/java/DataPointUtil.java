import java.io.IOException;

/**
 * Created by root on 5/30/17.
 */
public class DataPointUtil {


    private static void testFindData(TSDBClient client) throws IOException {
        // Find data from a minute ago to now
        long now = System.currentTimeMillis() / 1000 * 1000;
        FindDataPointRequest request = FindDataPointRequest.newBuilder()
                .addPoints(TSDBClient.newPoint(TEST_FACTORY_CODE, TEST_GROUP_CODE, TEST_POINT_CODE))
                .setInterpolation(true).setDownsampler(Downsampler.AVG).setInterval(1000)
                .setStartTimestamp(now - 60 * 1000)
                .setEndTimestamp(now).build();
        FindDataPointResponse response = client.findDataPoint(request);
        if (response.getStatus() && response.getDataPointsCount() > 0)
            System.out.println("Find data: \t success");
        else
            System.out.println("Find data: \t fail");
    }

    private void findDataPoint(TSDBClient client) throws IOException {

    }

}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import java.io.IOException;

public class HadoopStatusChecker {

    public static void main(String[] args) {
        // Check for optional configuration directory from command-line argument
        String configDir = null;
        if (args.length > 0) {
            configDir = args[0];
        }

        // Initialize Hadoop configuration
        Configuration conf = new Configuration();
        if (configDir != null) {
            conf.addResource(configDir + "/core-site.xml");
            conf.addResource(configDir + "/yarn-site.xml");
        }

        // Check HDFS status
        try {
            FileSystem fs = FileSystem.get(conf);
            System.out.println("HDFS is running.");
            fs.close();
        } catch (IOException e) {
            System.out.println("HDFS is not running: " + e.getMessage());
        }

        // Check YARN status
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        try {
            yarnClient.getClusterMetrics();
            System.out.println("YARN is running.");
        } catch (YarnException | IOException e) {
            System.out.println("YARN is not running: " + e.getMessage());
        } finally {
            yarnClient.stop();
        }
    }
}
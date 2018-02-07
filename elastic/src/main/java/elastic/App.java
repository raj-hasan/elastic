package elastic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws InterruptedException{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("listener", new MyListner());

        builder.setBolt("mybolt", new MyPrinter())
                //.shuffleGrouping("listener");
                .fieldsGrouping("listener", new Fields("line"));

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test-topology", config, topology);

        TimeUnit.MINUTES.sleep(1);
    }
}

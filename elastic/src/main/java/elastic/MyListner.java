package elastic;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import sun.misc.IOUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MyListner extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private List<String> lines;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        lines = Arrays.asList("a", "b", "c", "d");

    }

    public void nextTuple() {
        lines.stream().forEach(line -> spoutOutputCollector.emit(new Values(line)));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}

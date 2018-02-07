package elastic;

import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class MyPrinter extends BaseBasicBolt {
    private Map<String, Integer> counter;

    @Override
    public void prepare(Map config, TopologyContext context) {
        counter = new HashedMap();
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField("line");
        counter.put(word, countFor(word)+1);
        print();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private Integer countFor(String key){
        Integer count = counter.get(key);
        return count == null ? 0: count;
    }

    private void print(){
        counter.keySet()
                .forEach(word-> System.out.format("%s has count  = %s", word, counter.get(word)));
    }
}

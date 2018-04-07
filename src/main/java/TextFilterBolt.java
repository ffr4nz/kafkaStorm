import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.io.UnsupportedEncodingException;

public class TextFilterBolt extends BaseRichBolt  {
    OutputCollector _collector;
    private String FILTER_KEY="primavera";
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
        Object value = tuple.getValue(0);
        String sentence = null;
        if (value instanceof String) {
            sentence = (String) value;
        } else {
            // Kafka returns bytes
            byte[] bytes = (byte[]) value;
            try {
                sentence = new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        if(sentence.contains(FILTER_KEY))
            _collector.emit("stream1",new Values(sentence));
        else
            _collector.emit("stream2",new Values(sentence));
        
        _collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("stream1",new Fields("kafka_text"));
        declarer.declareStream("stream2", new Fields("kafka_text"));
    }
}

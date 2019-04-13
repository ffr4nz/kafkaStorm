import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.storm.tuple.Tuple;
import java.util.Map;
import java.util.Properties;

public class KafkaBolt extends BaseRichBolt {
    private Producer<String,String> producer;
    private String topicName;
    private OutputCollector outputCollector;
    
    public KafkaBolt(String topicName) {
        this.topicName = topicName;
    }

    public KafkaBolt() {
    }
    
    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<>(config);
        this.outputCollector=outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        KeyedMessage<String, String> data = new KeyedMessage<>(topicName, tuple.getStringByField("kafka_text"));
        producer.send(data);
        outputCollector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}

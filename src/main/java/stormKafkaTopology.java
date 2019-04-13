import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;


public class stormKafkaTopology {

    private static final String KAFKA_SPOUT = "KafkaSpout";
    private static final String FILTER_BOLT = "filterBolt";
    private static final String KAFKA_WRITE_BOLT = "KafkaWriteBolt";
    private static final String KAFKA_WRITE_ELSE_BOLT = "KafkaElseWriteBolt";
    private static final String TOPOLOGY_NAME = "MyTopologyKafkaStorm";

    public static void main(String[] args) throws Exception {
        int numSpoutExecutors = 1;
        KafkaSpout kspout;
        kspout = buildKafkaSentenceSpout();
        TextFilterBolt TFBolt = new TextFilterBolt();
        KafkaBolt KfBolt = new KafkaBolt("kafkatopic2");
        KafkaBolt KfBoltElse = new KafkaBolt("kafkatopic3");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT, kspout, numSpoutExecutors);
        builder.setBolt(FILTER_BOLT, TFBolt).shuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(KAFKA_WRITE_BOLT, KfBolt).shuffleGrouping(FILTER_BOLT,"stream1");
        builder.setBolt(KAFKA_WRITE_ELSE_BOLT, KfBoltElse).shuffleGrouping(FILTER_BOLT,"stream2");
        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
//            Utils.sleep(10000);
//            cluster.killTopology(TOPOLOGY_NAME);
//            cluster.shutdown();
        }
    }
    private static KafkaSpout buildKafkaSentenceSpout() {
        String zkHostPort = "localhost:2181";
        String topic = "kafkatopic1";
        String zkRoot = "/kafkaStormSpout";
        String zkSpoutId = KAFKA_SPOUT;
        ZkHosts zkHosts = new ZkHosts(zkHostPort);
        SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
        return new KafkaSpout(spoutCfg);
    }
}

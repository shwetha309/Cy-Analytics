package AggregateCount;

import classes.*;
import Serializer.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;

import java.util.*;

public class HourlyCountStateful {

    public static void main(String[] args) {

        StreamsConfig streamingConfig = new StreamsConfig(getProperties());

        TopologyBuilder builder = new TopologyBuilder();

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
  
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        
	Map < String, Object > serdeProps = new HashMap < > ();
        final Serializer < HourlyCategSummary > rsummarySerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", HourlyCategSummary.class);
        rsummarySerializer.configure(serdeProps, false);
 
        final Deserializer < HourlyCategSummary > rsummaryDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", HourlyCategSummary.class);
        rsummaryDeserializer.configure(serdeProps, false);
	final Serde < HourlyCategSummary > rsummarySerde = Serdes.serdeFrom(rsummarySerializer, rsummaryDeserializer); 

        builder.addSource("Region Data Process", stringDeserializer, rsummaryDeserializer,"CategCountStream")
                       .addProcessor("act-process", AggregateProcessor::new, "Region Data Process")
                       .addStateStore(Stores.create("AggCounts").withStringKeys()
                               .withValues(rsummarySerde).inMemory().maxEntries(100).build(),"act-process")
                       .addSink("sink", "stocks-out", stringSerializer, rsummarySerializer,"Region Data Process")
                       .addSink("sink-2", "AggCountStream", stringSerializer, rsummarySerializer, "act-process");

        System.out.println("Starting Activity Within Region Processor");
        KafkaStreams streaming = new KafkaStreams(builder, streamingConfig);
        streaming.start();
        

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Region Activity Within-Processor");
        props.put("group.id", "test-consumer-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful_processor_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}

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

import java.util.Properties;
import java.util.*;

public class AggregateRegionStateful {

    public static void main(String[] args) {

        StreamsConfig streamingConfig = new StreamsConfig(getProperties());

        TopologyBuilder builder = new TopologyBuilder();
	
	HashMap < String, Object > serdeProps = new HashMap < > ();
        final Serializer < HourlyCategSummary > csummarySerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", HourlyCategSummary.class);
        csummarySerializer.configure(serdeProps, false);
 
        final Deserializer < HourlyCategSummary > csummaryDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", HourlyCategSummary.class);
        csummaryDeserializer.configure(serdeProps, false);
	final Serde < HourlyCategSummary > csummarySerde = Serdes.serdeFrom(csummarySerializer, csummaryDeserializer); 

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
	
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
	
	        
	 
	System.out.println("Hello");
        builder.addSource("Live-Count", stringDeserializer, csummaryDeserializer,"CategCountStream")
                       .addProcessor("aggpcount", AggregateProcessor::new, "Live-Count")
                       .addStateStore(Stores.create("Agg-Counts").withStringKeys()
                               .withValues(csummarySerde).inMemory().maxEntries(100).build(),"aggpcount")
                       .addSink("sink", "stocks-out", stringSerializer,csummarySerializer,"Live-Count")
                       .addSink("sink-2", "AggCountStream", stringSerializer, csummarySerializer, "aggpcount");

        System.out.println("Starting StockSummaryStatefulProcessor Example");
        KafkaStreams streaming = new KafkaStreams(builder, streamingConfig);
        streaming.start();
        System.out.println("StockSummaryStatefulProcessor Example now started");

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "AggregateCountStateful-Processor");
        props.put("group.id", "test-consumer-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful_processor_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}

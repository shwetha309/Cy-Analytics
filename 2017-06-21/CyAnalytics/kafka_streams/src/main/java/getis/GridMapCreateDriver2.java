package getis;

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
import org.apache.kafka.streams.processor.*;
import java.util.*;


public class GridMapCreateDriver2 {
	public static void main(String args[]) {
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "grid-map-key");
  		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  		streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
  		streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		
		TopologyBuilder builder = new TopologyBuilder();

        	StringSerializer stringSerializer = new StringSerializer();
		StringDeserializer stringDeserializer = new StringDeserializer();	
		
		//KStreamBuilder builder = new KStreamBuilder();
		
		Map < String, Object > serdeProps = new HashMap < > ();
	        final Serializer < GridMap > gmapSerializer = new JsonPOJOSerializer < > ();
        	serdeProps.put("JsonPOJOClass", GridMap.class);
        	gmapSerializer.configure(serdeProps, false);
 
        	final Deserializer < GridMap > gmapDeserializer = new JsonPOJODeserializer < > ();
        	serdeProps.put("JsonPOJOClass", GridMap.class);
        	gmapDeserializer.configure(serdeProps, false);
		final Serde < GridMap > gmapSerde = Serdes.serdeFrom(gmapSerializer, gmapDeserializer); 

		
		//Map < String, Object > serdeProps = new HashMap < > ();
	        final Serializer < RegionSummary > rsummarySerializer = new JsonPOJOSerializer < > ();
        	serdeProps.put("JsonPOJOClass", RegionSummary.class);
        	rsummarySerializer.configure(serdeProps, false);
 
        	final Deserializer < RegionSummary > rsummaryDeserializer = new JsonPOJODeserializer < > ();
        	serdeProps.put("JsonPOJOClass", RegionSummary.class);
        	rsummaryDeserializer.configure(serdeProps, false);
		final Serde < RegionSummary > rsummarySerde = Serdes.serdeFrom(rsummarySerializer, rsummaryDeserializer); 

		//KStream<String, RegionSummary> rsummaryStream= builder.stream(stringSerde, rsummarySerde, "RegionDataStreams");
		
		StateStoreSupplier GScoreStore = Stores.create("GScore")
						.withKeys(Serdes.String())
    						.withValues(Serdes.Long())
    						.inMemory()
    						.build();
		
		//KStream<String, Long> transformed = input.transform(/* your TransformerSupplier */, countStore.name());
		/*builder.addSource("Region Data Process", stringDeserializer, rsummaryDeserializer,"RegionDataStreams")
                       .addProcessor("gscore-process", GScoreProcess::new, "Region Data Process")
                       	.addStateStore(GScoreStore, "gscore-process")
			.connectProcessorAndStateStores("gscore-process", "GScore")
			.connectProcessorAndStateStores("gscore-process", "GridMap")
			.addSink("SINK1", "Gscore-Out", "gcore-process");
		//KStream<String, RegionSummary> rsummaryStream= builder.stream(stringSerde, rsummarySerde, "RegionDataStreams");
		
		/*StateStoreSupplier countStore = Stores.create("GridMap")
						.withKeys(Serdes.String())
    						.withValues(Serdes.Long())
    						.persistent()
    						.build();*/
		
		//KStream<String, Long> transformed = input.transform(/* your TransformerSupplier */, countStore.name());
		builder.addSource("Region Data Process", stringDeserializer, stringDeserializer,"Grid-Initial")
                      .addSource("Region Data Process2", stringDeserializer, rsummaryDeserializer,"RegionDataStreams")
			.addProcessor("create grid-process", GridMapProcess::new, "Region Data Process")
                       .addStateStore(Stores.create("GridMapStore").withStringKeys()
		       .withValues(gmapSerde).persistent().build(),"create grid-process")	
                       .addProcessor("gscore-process", GScoreProcess::new, "Region Data Process2")
                       	.addStateStore(GScoreStore, "gscore-process")
			.connectProcessorAndStateStores("gscore-process", "GScore")
			.connectProcessorAndStateStores("gscore-process", "GridMapStore")
			.addSink("SINK1", "Gscore-Out", "gscore-process");

		System.out.println("Starting GridMap Processor");
        	KafkaStreams streaming = new KafkaStreams(builder, streamsConfiguration);
		streaming.start();

	}
}

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


public class GridMapCreateDriver {
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

		//TopologyBuilder builder2 = new TopologyBuilder();
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

		final Serializer < RegionSummary > rsummarySerializer = new JsonPOJOSerializer < > ();
        	serdeProps.put("JsonPOJOClass", RegionSummary.class);
        	rsummarySerializer.configure(serdeProps, false);
 
	        final Deserializer < RegionSummary > rsummaryDeserializer = new JsonPOJODeserializer < > ();
        	serdeProps.put("JsonPOJOClass", RegionSummary.class);
        	rsummaryDeserializer.configure(serdeProps, false);
		final Serde < RegionSummary > rsummarySerde = Serdes.serdeFrom(rsummarySerializer, rsummaryDeserializer); 
		
		//KStream<String, RegionSummary> rsummaryStream= builder.stream(stringSerde, rsummarySerde, "RegionDataStreams");
		
		StateStoreSupplier GridMapStore = Stores.create("GridMapStore")
						.withKeys(Serdes.String())
    						.withValues(gmapSerde)
    						.persistent()
    						.build();
		StateStoreSupplier GScoreStore = Stores.create("GScoreStore")
						.withKeys(Serdes.String())	
						.withValues(Serdes.Double())
						.inMemory()
						.build();
		StateStoreSupplier AttackStore = Stores.create("AttackStore")
						.withKeys(Serdes.String())
						.withValues(Serdes.Long())
						.inMemory()
						.build();
		
			
	/*	builder.addSource("Grid-Initial", stringDeserializer, stringDeserializer,"Grid-Initial")
		 //      .addSource("RegionDataStreams", stringDeserializer, rsummaryDeserializer, "RegionDataStreams")
                       .addProcessor("Process1", GridMapProcess::new, "Grid-Initial")
		   //    .addProcessor("Process2", GScoreProcess::new, "RegionDataStreams")
                       .addStateStore(GridMapStore,"Process1");*/
		     //  .addStateStore(AttackStore, "Process2")
		      // .addStateStore(GScoreStore, "Process2")
		     //  .connectProcessorAndStateStores("Process2", "GridMapStore")
		     // .addSink("SINK2", "GOutput", "Process2");
	
		//KStream<String, Long> transformed = input.transform(/* your TransformerSupplier */, countStore.name());
		builder.addSource("Grid-Initial", stringDeserializer, stringDeserializer,"Grid-Initial")
			.addSource("RegionDataStreams", stringDeserializer, rsummaryDeserializer, "RegionDataStreams")
                       .addProcessor("Process1", GridMapProcess::new, "Grid-Initial")
		       .addProcessor("Process2", GScoreProcess::new, "RegionDataStreams")
                       .addStateStore(GridMapStore,"Process1")
		       .addStateStore(AttackStore, "Process2")
		       .addStateStore(GScoreStore, "Process2")
		       .connectProcessorAndStateStores("Process2", "GridMapStore")
		       .addSink("SINK2", "GScoreOutput", "Process2");

		System.out.println("Starting GridMap Processor");
        	KafkaStreams streaming = new KafkaStreams(builder, streamsConfiguration);
		streaming.start();

	}
}

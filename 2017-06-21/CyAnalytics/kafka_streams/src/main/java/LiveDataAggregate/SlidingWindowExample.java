package LiveDataAggregate;

import java.util.Properties;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class SlidingWindowExample {

	public static void main(String[] args) {
		System.out.println("hey");	
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "Letters Slidiing Window Trial");
  		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  		streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
  		streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		StringSerializer stringSerializer = new StringSerializer();
		StringDeserializer stringDeserializer = new StringDeserializer();

		WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
        	WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
		Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);

		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> lettersStream = builder.stream(stringSerde, stringSerde, "Letters");
		lettersStream.foreach( new ForeachAction < String, String > () {
			public void apply( String key, String value) {
				System.out.println(key+" "+value);
				//System.out.print("lol");
			}
		});
		
		KTable<Windowed<String>, Long> tempTable = lettersStream.map((k,v) -> new KeyValue<> ((v.split(" "))[1],(v.split(" ")[1]))).groupByKey()
		.aggregate(() -> 0L,(aggKey, value, aggregate) -> aggregate+1L,
                TimeWindows.of(10000).advanceBy(1*1000),longSerde,"summary");

		
		/*tempTable.foreach( new ForeachAction < String, Long > () {
			public void apply( String key, Long value) {
				System.out.println(key+" "+value);
				//System.out.print("lol");
			}
		});*/
		tempTable.print();
		//tempTable.to(windowedSerde,longSerde,"CategCountStream");

		KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
  		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
		


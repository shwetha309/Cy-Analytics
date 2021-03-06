package LiveDataAggregate;

/**
 * Hello world!
 *
 */
import classes.*;
import Serializer.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.*;
import java.util.*;

public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
	Properties streamsConfiguration = new Properties();
  	streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "AggregateCount-ActiveHour");
  	streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  	streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
  	streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
  	streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

  	final Serde<String> stringSerde = Serdes.String();
  	final Serde<Long> longSerde = Serdes.Long();
   
  	Map < String, Object > serdeProps = new HashMap < > ();
        final Serializer < AttackMessage > attackMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", AttackMessage.class);
        attackMessageSerializer.configure(serdeProps, false);
 
        final Deserializer < AttackMessage > attackMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", AttackMessage.class);
        attackMessageDeserializer.configure(serdeProps, false);
        final Serde < AttackMessage > attackMessageSerde = Serdes.serdeFrom(attackMessageSerializer, attackMessageDeserializer);
   	
	KStreamBuilder builder = new KStreamBuilder();
	KStream<String, AttackMessage> attackStream= builder.stream(stringSerde, attackMessageSerde, "cyberwar");

  	KStream<String, AttackCategory> categKStream = attackStream
        					      .map((k, v) -> new KeyValue<>(v.attack_type.toString(), new AttackCategory(v.latitude,v.longitude,v.city_target,v.country_target)));
  	
	KTable<String,Long> categ_count = categKStream.map((k,v) -> new KeyValue<> (k,k)).groupBy((key,value)->key).count("Counts");
    	categ_count.to(stringSerde, longSerde, "CategCountStream");

	
	KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
  	streams.start();

	Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


     }
}

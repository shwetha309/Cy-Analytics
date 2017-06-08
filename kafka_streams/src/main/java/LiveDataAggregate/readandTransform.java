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

public class readandTransform 
{
    public static void main( String[] args )
    {
        System.out.println( "Reading and Aggregating Data" );
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
   		
        final Serializer <RegionSummary > rsSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", RegionSummary.class);
        rsSerializer.configure(serdeProps, false);
 
        final Deserializer < RegionSummary > rsDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", RegionSummary.class);
        rsDeserializer.configure(serdeProps, false);
        final Serde < RegionSummary > rsSerde = Serdes.serdeFrom(rsSerializer, rsDeserializer);

	KStreamBuilder builder = new KStreamBuilder();
	KStream<String, AttackMessage> attackStream= builder.stream(stringSerde, attackMessageSerde, "cyberwarInput");

  	KStream<String, AttackCategory> categKStream = attackStream
        					      .map((k, v) -> new KeyValue<>(v.attack_type.toString(), new AttackCategory(v.latitude,v.longitude,v.city_target,v.country_target)));
  	
	KTable<String,Long> categ_count = categKStream.map((k,v) -> new KeyValue<> (k,k)).groupBy((key,value)->key).count("Counts");
    	categ_count.to(stringSerde, longSerde, "CategCountStream");

	
	KStream<String, RegionSummary> actKStream = attackStream.map((k,v) -> new KeyValue<>(Double.toString(v.latitude)+"_"+Double.toString(v.longitude),	
new RegionSummary(v.attack_type,v.attack_subtype,v.timestamp, v.latitude, v.longitude, v.country_target, v.city_target)));
	
	/*actKStream.foreach(new ForeachAction<String, AttacksWithinRegion>() {
    		public void apply(String key, AttacksWithinRegion value) {
        		System.out.println(key + ": " + value);
    		}
	});*/
         
	actKStream.through(stringSerde, rsSerde,"RegionDataStream");
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
  	streams.start();

	Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


     }
}

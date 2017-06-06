package kafka_streams;

/**
 * Hello world!
 *
 */
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
//import org.apache.kafka.streams.kstream.KeyValue;

import java.util.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
//import org.codehaus.jackson.map.DeserializationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

class AttackMessage {
        
        public String attack_type;
	public int ddos_attack_length;
        public String attacker_ip;
        public Object attack_port;
        public double latitude;
        public double latitude2;
        public Object country_target;
	public double longitude2;
	public double longitude;
	public String attacker;
	public String attack_subtype;
	public String city_target;
	public String city_origin;
        public String country_origin;
        public Object ddos_origin_port;
        public Object ddos_origin;
        public Object ddos_attack_bps;
}

class AttackCategory
{
  	public double latitude;
	public double longitude;
	public String city;
	public Object country;
	
	public AttackCategory(double lat, double lon, String cty, Object ctry)
	{
		latitude = lat;
		longitude = lon;
		city = cty;
		country = ctry;
	}

	public String getCountry()
	{
		String ctry=this.country.toString().replace("[","").replace("]","").replace("\"","");
		return ctry;
	}
}

class JsonPOJOSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {
    }
    
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}
class JsonPOJODeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();//;.configure(DeserializationConfig.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    private Class<T> tClass;
    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJODeserializer() {
    }
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get("JsonPOJOClass");
    }
    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;
        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
        return data;
    }
    @Override
    public void close() {
    }
}
public class App2 
{
    //static final String SUM_OF_ODD_NUMBERS_TOPIC = "sum-of-odd-numbers-topic";
    //static final String NUMBERS_TOPIC = "numbers-topic";
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Properties streamsConfiguration = new Properties();
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
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
  //KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "TextLinesTopic");
  //KTable<String, Long> wordCounts = textLines
 //       .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        //.map((key, word) -> new KeyValue<>(word, word))
        // Required in Kafka 0.10.0 to re-partition the data because we re-keyed the stream in the `map` step.
        // Upcoming Kafka 0.10.1 does this automatically for you (no need for `through`).
        //.through("RekeyedIntermediateTopic")
        //.countByKey("Counts")
        //.toStream();
   //     .groupBy((key,word)->word)
     //   .count("Counts");
  //wordCounts.to(stringSerde, longSerde, "WordsWithCountsTopic");

  KStream<String, AttackMessage> attackStream= builder.stream(stringSerde, attackMessageSerde, "cyberwar");

  KStream<String, AttackMessage> sensorDataKStream = attackStream
            .map((k, v) -> new KeyValue<>(v.attack_type.toString(), v));

    
  KStream<String, AttackCategory> categKStream = attackStream
            .map((k, v) -> new KeyValue<>(v.attack_type.toString(), new AttackCategory(v.latitude,v.longitude,v.city_target,v.country_target)));
  KStream<String, AttackCategory> subategKStream = attackStream
            .map((k, v) -> new KeyValue<>(v.attack_subtype.toString(), new AttackCategory(v.latitude,v.longitude,v.city_target,v.country_target)));

KTable<String,Long> categ_count = categKStream.map((k,v) -> new KeyValue<> (k,k)).groupBy((key,value)->key).count("Counts");

  /*categ_count.foreach(new ForeachAction<String, Long>() {
    public void apply(String key, Long value) {
        System.out.println(key + ": " + value);
    }
 });*/
 //categ_count.to(stringSerde,longSerde,"categcount");
KTable<String,Long> categ_region_count = (categKStream.map((k,v) -> new KeyValue (k+"_"+v.getCountry(),k+"_"+v.getCountry())).groupBy((k,v)->k).count("RCounts"));

 categ_region_count.to(stringSerde,longSerde,"categcount");

 /* KStream<String> cmap_class = countriesStream.flatMapValues(new ValueMapper<String, Iterable<CountryMessage>>() {
    @Override
    public Iterable<CountryMessage> apply(CountryMessage value) {
        ArrayList<String> keywords = new ArrayList<String>();
	keywords.add(CountryMessage.attack_type);
        // apply regex to value and for each match add it to keywords

        return keywords;
    }
});
//System.out.println(countriesStream);  
  //KTable<String,Long> runningCountriesCountPerContinent = countriesStream.groupBy((k, country)-> country.attack_type).count("Counts");
  //runningCountriesCountPerContinent.to(stringSerde, longSerde,  "RunningCountryCountPerContinent");
  //runningCountriesCountPerContinent.print(stringSerde, longSerde); 
*/  
  KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
  streams.start();

Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

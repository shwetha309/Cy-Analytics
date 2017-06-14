package kafka_streams;

/**
 * Hello world!
 *
 */

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
public class App 
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

  KStreamBuilder builder = new KStreamBuilder();
  KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "TextLinesTopic");
  KTable<String, Long> wordCounts = textLines
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        //.map((key, word) -> new KeyValue<>(word, word))
        // Required in Kafka 0.10.0 to re-partition the data because we re-keyed the stream in the `map` step.
        // Upcoming Kafka 0.10.1 does this automatically for you (no need for `through`).
        //.through("RekeyedIntermediateTopic")
        //.countByKey("Counts")
        //.toStream();
        .groupBy((key,word)->word)
        .count("Counts");
  wordCounts.to(stringSerde, longSerde, "WordsWithCountsTopic");

  KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
  streams.start();

Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

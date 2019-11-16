import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * this is not working properly
 * @author P.K OJHA
 *
 */

public class kafkaSpark{
static Map<String, Object> kafkaParams = new HashMap<>();


    public static void main(String[] args) throws InterruptedException, StreamingQueryException {
    	
    	  Logger.getLogger("org").setLevel(Level.OFF);
    	  Logger.getLogger("akka").setLevel(Level.OFF);
    	  
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Sampleapp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "1");
        kafkaParams.put("auto.offset.reset", "earliest"); // from-beginning?
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test1");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        System.out.println("Direct Stream created? ");
        stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
                        System.out.println("record key : "+record.key()+" value is : "+record.value());
                        return new Tuple2<>(record.key(), record.value());
                    }
                });
        System.out.println("Reached the end.");
        stream.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
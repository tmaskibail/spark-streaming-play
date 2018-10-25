package com.urthilak.spark.play;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HelloSpark {

    public static final Logger LOGGER = LoggerFactory.getLogger(HelloSpark.class);

    public static void main(String[] args) throws InterruptedException {

        // Create context with a 1 seconds batch interval
        SparkConf sparkConf = new SparkConf()
                .setAppName("PopularInstrument")
                .setMaster("local[2]")
                .set("spark.executor.memory", "1g");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "sparkStreamingGroup");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList("order"), kafkaParams));


        // Process last 5 seconds of data, every 3 seconds
        JavaDStream<String> isins = messages
                .map((Function<ConsumerRecord<String, String>, String>) record -> record.value());


        // Count ISINs in  each batch
        JavaPairDStream<String, Integer> isinsPairs = isins.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        );


        // Cumulate the sum from each batch
        JavaPairDStream<String, Integer> isinCounts = isinsPairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2
        );

//        popularIsins.map(new Function<Tuple2<String, Long>, Object>() {
//            @Override
//            public Object call(Tuple2<String, Long> agrRecord) throws Exception {
//
//                LOGGER.info("ISIN : {}, count : {}", agrRecord._1(), agrRecord._2());
//                return null;
//            }
//        });


        isinCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}

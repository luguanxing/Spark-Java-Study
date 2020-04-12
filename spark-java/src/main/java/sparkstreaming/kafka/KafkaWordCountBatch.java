package sparkstreaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaWordCountBatch {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaWordCountBatch")
                .setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
        // 对接kafka
        Map kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaWordCountBatch");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        Set<String> topics = Stream.of("test").collect(Collectors.toSet());
        // 获取行kafka数据流，取出行数据并拆分单词流
        JavaInputDStream<ConsumerRecord<String, String>> dataDStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));
        JavaDStream<String> lineDStream = dataDStream.map(ConsumerRecord::value);
        JavaDStream<String> wordDStream = lineDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // 词频统计，统计每个批次的数据
        wordDStream
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .print();
        // 启动并阻塞等待运行
        jssc.start();
        jssc.awaitTermination();
    }

}

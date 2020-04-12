package sparkstreaming.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Objects;

public class NcWordCountGlobalTTL {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf()
                .setAppName("NcWordCountGlobalTTL")
                .setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
        jssc.checkpoint("checkpoint-dir");
        // 获取行socket流，拆分单词流
        JavaReceiverInputDStream<String> lineDStream = jssc.socketTextStream("127.0.0.1", 12345);
        JavaDStream<String> wordDStream = lineDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // 词频统计，统计全局的数字但只输出更新的部分，另外对每个单词设置TTL
        Integer ttl = 10;
        wordDStream
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapWithState(
                        StateSpec.function(
                                (Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>) (word, value, state) -> {
                                    Integer currentValue = value.orElse(0);
                                    Integer currentSum = state.exists() ? currentValue + state.get() : currentValue;
                                    if (state.isTimingOut()) {
                                        // 到ttl清除统计状态
                                        System.err.println("[" + word + "] expired");
                                        return null;
                                    } else {
                                        // 未到ttl更新统计状态
                                        state.update(currentSum);
                                        return new Tuple2<>(word, currentSum);
                                    }
                                }
                        ).timeout(Durations.seconds(ttl))
                )
                .filter(Objects::nonNull)
                .print();

        // 启动并阻塞等待运行
        jssc.start();
        jssc.awaitTermination();
    }


}
package sparkstreaming.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class NcWordCountGlobal {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf()
                .setAppName("NcWordCountGlobal")
                .setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
        jssc.checkpoint("checkpoint-dir");
        // 获取行socket流，拆分单词流
        JavaReceiverInputDStream<String> lineDStream = jssc.socketTextStream("127.0.0.1", 12345);
        JavaDStream<String> wordDStream = lineDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // 词频统计，统计全局的数字
        wordDStream
                .mapToPair(word -> new Tuple2<>(word, 1))
                .updateStateByKey(
                        (Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (currentValues, lastValue) -> {
                            // lastValues是当前批次Key的全部Value
                            Integer currentSum = currentValues
                                    .stream()
                                    .reduce(Integer::sum)
                                    .orElse(0);
                            Integer newSum = lastValue.orElse(0) + currentSum;
                            return Optional.of(newSum);
                        }
                )
                .print();
        // 启动并阻塞等待运行
        jssc.start();
        jssc.awaitTermination();
    }

}
package sparkstreaming.window;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class NcWordCountBatchWindow {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf()
                .setAppName("NcWordCountBatchWindow")
                .setMaster("local[*]");
        Duration interval = Durations.seconds(5);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, interval);
        jssc.sparkContext().setLogLevel("ERROR");
        // 获取行socket流，拆分单词流
        JavaReceiverInputDStream<String> lineDStream = jssc.socketTextStream("127.0.0.1", 12345);
        JavaDStream<String> wordDStream = lineDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // 词频统计，统计每个时间窗口批次的数据，必须是间隔时间的整数倍
        Duration window = interval.times(2);
        Duration slide = interval.times(1);
        wordDStream
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKeyAndWindow(Integer::sum, window, slide)
                .print();
        // 启动并阻塞等待运行
        jssc.start();
        jssc.awaitTermination();
    }

}

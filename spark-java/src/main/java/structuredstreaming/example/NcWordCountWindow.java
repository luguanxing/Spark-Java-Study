package structuredstreaming.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NcWordCountWindow {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("NcWordCountWindow")
                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");

        // 读入带时间戳的流数据处理dataframe
        Dataset<Row> lineTsDF = sparkSession
                .readStream()
                .format("socket")
                .option("host", "127.0.0.1")
                .option("port", 12345)
                .option("includeTimestamp", true)
                .load();

        // 转化为时间戳的单词数据dataframe
        Dataset<Tuple2<String, Timestamp>> lineTsDS = lineTsDF.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()));
        Dataset<Tuple2<String, Timestamp>> wordTsDS = lineTsDS.flatMap(
                tuple2 -> {
                    String line = tuple2._1;
                    Timestamp timestamp = tuple2._2;
                    return Stream.of(line.split(" "))
                            .map(word -> new Tuple2<>(word, timestamp))
                            .collect(Collectors.toSet())
                            .iterator();
                },
                Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
        );
        Dataset<Row> wordTsDF = wordTsDS.toDF("word", "ts");

        // 映射到同一窗口时间范围内去重
        String timeUnit = "10 seconds";
        String windowDuration = timeUnit;
        String slideDuration = timeUnit;
        Dataset<Row> wordDF = wordTsDF
                .withWatermark("ts", timeUnit)
                .withColumn("window", functions.window(functions.col("ts"), windowDuration, slideDuration))
                .drop("ts")
                .dropDuplicates("window", "word");

        // 进行词频统计并以更新模式输出新的结果
        StreamingQuery query = wordDF
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();
        query.awaitTermination();

        // 关闭资源
        jsc.stop();
    }

}

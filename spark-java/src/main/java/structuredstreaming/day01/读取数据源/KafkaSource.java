package structuredstreaming.day01.读取数据源;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class KafkaSource {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("KafkaSource")
                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");

        // 读取kafka数据，注意checkpoint中已经维护了offset，不需要再使用groupid
        Dataset<Row> kafkaDF = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", " 127.0.0.1:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "earliest")
                .load();
        kafkaDF.printSchema();

        // 转化为数据dataset
        Dataset<Row> lineDF = kafkaDF.selectExpr("CAST(value AS STRING) as value");
        Dataset<String> lineDS = lineDF.as(Encoders.STRING());
        Dataset<String> wordDS = lineDS.flatMap(line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());

        // 统计数据并输出
        Dataset<Row> wordcountDF = wordDS
                .groupBy("value")
                .count();
        StreamingQuery query = wordcountDF
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();
        query.awaitTermination();

        // 关闭资源
        jsc.stop();
    }


}

package structuredstreaming.day02数据落地;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;

public class KafkaSink {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("KafkaSink")
                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");

        // 读入流数据
        Dataset<Row> lineDF = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                .option("subscribe", "test_input")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as value");

        // 数据追加落地到kafka
        String checkPointPath = "/Users/luguanxing/data/structuredStream_KafkaCheckPoint";
        lineDF
                .writeStream()
                .format("kafka")
                .outputMode(OutputMode.Append())
                .option("checkpointLocation", checkPointPath)
                .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                .option("topic", "test_output")
                .start()
                .awaitTermination();

        // 关闭资源
        jsc.stop();
    }

}

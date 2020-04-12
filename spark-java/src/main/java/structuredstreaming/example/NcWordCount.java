package structuredstreaming.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class NcWordCount {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("NcWorkCount")
                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");

        // 读入流数据处理dataframe
        Dataset<Row> lineDF = sparkSession
                .readStream()
                .format("socket")
                .option("host", "127.0.0.1")
                .option("port", 12345)
                .load();

        // 转化为数据dataset
        Dataset<String> lineDS = lineDF.as(Encoders.STRING());
        Dataset<String> wordDS = lineDS.flatMap(line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());

        // 进行词频统计并输出结果
        Dataset<Row> wordcountDF = wordDS
                .groupBy("value")
                .count();
        StreamingQuery query = wordcountDF
                .writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();
        query.awaitTermination();

        // 关闭资源
        jsc.stop();
    }

}

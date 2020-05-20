package structuredstreaming.day01.读取数据源;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public class HDFSSource {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("HDFSSource")
                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");

        // 读取HDFS数据并使用shcema解析
        String path = "hdfs://127.0.0.1:8020/dataset/";
        Dataset<Row> dataDF = sparkSession
                .readStream()
                .text(path);

        // 输出
        StreamingQuery query = dataDF
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();
        query.awaitTermination();

        // 关闭资源
        jsc.stop();
    }


}

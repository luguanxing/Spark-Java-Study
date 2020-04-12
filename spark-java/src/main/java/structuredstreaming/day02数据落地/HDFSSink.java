package structuredstreaming.day02数据落地;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;

import java.util.Arrays;

public class HDFSSink {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("HDFSSink")
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

        // 转化为数据dataset，并进行词频统计
        Dataset<String> lineDS = lineDF.as(Encoders.STRING());
        Dataset<String> wordDS = lineDS.flatMap(line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());

        // 将结果落地到DFS，需要设置检查点，注意非首次运行要重新checkpointLocation删除文件夹
        String path = "/Users/luguanxing/data";
        wordDS
                .writeStream()
                .format("csv")
                .option("path", path + "/output")
                .option("checkpointLocation", path + "/checkpoint")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();

        // 关闭资源
        jsc.stop();
    }


}

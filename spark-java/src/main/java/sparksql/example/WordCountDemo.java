package sparksql.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Stream;

public class WordCountDemo {

    @Test
    public void sparkWordCount() {
        // 使用原生RDD实现wordcount
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCountDemo");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
        // 词频统计
        String filePath = "src/main/resources/wordcount";
        jsc.textFile(filePath)
                .flatMap(line -> Stream.of(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .collect()
                .forEach(System.out::println);
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void sparkSqlWordCount() {
        // 使用sparksql实现wordcount
        SparkSession sparkSession = SparkSession.builder()
                .appName("WordCountDemo")
                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 词频统计
        String path = "src/main/resources/wordcount";
        sparkSession
                .read()
                .textFile(path)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING())
                .groupBy("value")
                .count()
                .toDF("word", "cnt")
                .where("cnt >= 2")
                .orderBy(functions.desc("cnt"))
                .show();
        // 关闭资源
        jsc.stop();
    }

}

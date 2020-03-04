package spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.stream.Stream;

public class WordCount {

    public static void main(String[] args) {
        // 创建spark相关的context
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCount");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");

        // 从文件读入行rdd
        String filePath = "src/main/resources/wordcount";
        JavaRDD<String> lineRdd = jsc.textFile(filePath);

        // 转换成单词rdd
        JavaRDD<String> wordRdd = lineRdd.flatMap(line -> Stream.of(line.split(" ")).iterator());

        // 进行单词计数
        JavaPairRDD<String, Integer> wordCountRdd = wordRdd
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        // 输出结果
        wordCountRdd.collect().forEach(System.out::println);

        // 查看逻辑执行图
        System.out.println(wordCountRdd.toDebugString());

        // 关闭资源
        jsc.stop();
    }

}

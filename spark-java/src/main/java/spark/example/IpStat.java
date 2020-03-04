package spark.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class IpStat {

    public static void main(String[] args) {
        // 创建spark相关的context
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("IpStat");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");

        // 从文件读入行rdd
        String filePath = "src/main/resources/access_log_sample.txt";
        JavaRDD<String> lineRdd = jsc.textFile(filePath);

        // 抽取出IP并清洗数据
        JavaRDD<String> ipRdd = lineRdd
                .map(line -> line.split(" ")[0])
                .filter(StringUtils::isNotEmpty);

        // 按ip聚合（注意JavaPairRDD到JavaRDD的转换）
        JavaPairRDD<String, Integer> ipCountRdd = ipRdd
                .mapToPair(ip -> new Tuple2<>(ip, 1))
                .reduceByKey(Integer::sum);
        JavaRDD<Tuple2<String, Integer>> tuple2Rdd = JavaRDD.fromRDD(JavaPairRDD.toRDD(ipCountRdd), ipCountRdd.classTag());

        // 按统计次数排序并输出结果
        JavaRDD<Tuple2<String, Integer>> sortedRdd = tuple2Rdd.sortBy(tuple2 -> tuple2._2, false, 1);
        sortedRdd.take(10).forEach(tuple2 -> System.err.println(tuple2._1 + " -> " + tuple2._2));

        // 关闭资源
        jsc.stop();
    }

}

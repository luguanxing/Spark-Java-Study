package spark.day02.Rdd分区和Shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RddPartitionsDemo {

    private SparkConf sparkConf = null;
    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 创建spark相关的context
        sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("ActionDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void DefinePartitionsNum() {
        // 可指定分区数，注意textFile是最小分区数，实际可能更大
        JavaRDD<Integer> partitions5 = jsc.parallelize(Stream.of(1, 2, 3).collect(Collectors.toList()), 5);
        JavaRDD<Integer> partitions2 = jsc.parallelize(Stream.of(1, 2, 3).collect(Collectors.toList()), 2);
        JavaRDD<String> partitions3p = jsc.textFile("src/main/resources/access_log_sample.txt", 3);
        System.err.println(partitions5.partitions().size());
        System.err.println(partitions2.partitions().size());
        System.err.println(partitions3p.partitions().size());
    }

    @Test
    public void ChangePartitionsNum() {
        // 改变分区数，增大需要分区需要shuffle
        JavaRDD<Integer> rdd = jsc.parallelize(Stream.of(1, 2, 3).collect(Collectors.toList()), 6);
        JavaRDD<Integer> rdd1 = rdd.coalesce(1);
        JavaRDD<Integer> rdd2 = rdd.coalesce(10);
        JavaRDD<Integer> rdd3 = rdd.coalesce(15, true);
        JavaRDD<Integer> rdd4 = rdd.repartition(1);
        JavaRDD<Integer> rdd5 = rdd.repartition(8);
        System.err.println(rdd.partitions().size());
        System.err.println(rdd1.partitions().size());
        System.err.println(rdd2.partitions().size());
        System.err.println(rdd3.partitions().size());
        System.err.println(rdd4.partitions().size());
        System.err.println(rdd5.partitions().size());
    }

    @Test
    public void fuctionPartitions() {
        // 操作算子中指定分区数
        jsc.parallelize(Stream.generate(Math::random).limit(30).collect(Collectors.toList()))
                .sortBy(d -> d, false, 25)
                .map(d -> (int) (d * 10000))
                .collect()
                .forEach(System.err::println);
        jsc.parallelize(Stream.generate(Math::random).limit(30).collect(Collectors.toList()))
                .mapToPair(d -> new Tuple2<>(d, d))
                .sortByKey(true, 5)
                .keys()
                .collect()
                .forEach(System.out::println);

    }


}

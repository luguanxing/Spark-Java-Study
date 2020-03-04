package spark.day01.Rdd算子;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TypeRddDemo {

    private SparkConf sparkConf = null;
    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 创建spark相关的context
        sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("TypeRddDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void test() {
        // 三种数据类型的RDD提供了不同的支持
        JavaRDD<Integer> rdd = jsc.parallelize(
                Stream.iterate(1, i -> i + 1)
                        .limit(10)
                        .collect(Collectors.toList())
        );
        JavaPairRDD<String, Integer> pairRDD = JavaPairRDD.fromJavaRDD(
                jsc.parallelize(
                        Stream.of(
                                new Tuple2<>("手机", 1000),
                                new Tuple2<>("手机", 2000),
                                new Tuple2<>("电脑", 5000)
                        ).collect(Collectors.toList())
                ));
        JavaDoubleRDD doubleRdd = rdd.mapToDouble(i -> i);
        // 查看各自类型的一些支持
        rdd.collect().forEach(System.err::println);
        pairRDD.groupByKey().collect().forEach(System.out::println);
        System.err.println(doubleRdd.sum());
    }

}

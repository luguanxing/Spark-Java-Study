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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ActionDemo {

    private SparkConf sparkConf = null;
    private JavaSparkContext jsc = null;
    private JavaRDD<Integer> rdd = null;
    private JavaPairRDD<String, Integer> pairRDD = null;
    private JavaDoubleRDD doubleRdd = null;

    @Before
    public void before() {
        // 创建spark相关的context
        sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("ActionDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
        // 模拟数据
        rdd = jsc.parallelize(Stream.iterate(1, i -> i + 1).limit(10).collect(Collectors.toList()));
        pairRDD = JavaPairRDD.fromJavaRDD(jsc.parallelize(Stream.of(
                new Tuple2<>("手机", 1000),
                new Tuple2<>("手机", 2000),
                new Tuple2<>("电脑", 5000))
                .collect(Collectors.toList())));
        doubleRdd = rdd.mapToDouble(i -> i);
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void reduce() {
        // 统计求和
        Integer rddSum = rdd.reduce(Integer::sum);
        Tuple2<String, Integer> pairRddSum = pairRDD.reduce((t1, t2) -> new Tuple2<>("总价", t1._2 + t2._2));
        System.err.println(rddSum);
        System.err.println(pairRddSum);
    }

    @Test
    public void collect() {
        // 返回数组
        List<Integer> list = rdd.collect();
        list.forEach(System.err::println);
    }

    @Test
    public void foreach() {
        // 操作每条数据
        rdd.foreach(i -> System.out.println(i));
    }

    @Test
    public void count() {
        // 计算数量
        long count = rdd.count();
        Map<String, Long> countByKey = pairRDD.countByKey();
        System.err.println(count);
        System.err.println(countByKey);
    }

    @Test
    public void others() {
        // 其它一些操作
        System.err.println(rdd.first());
        System.err.println(rdd.take(3));
        System.err.println(rdd.takeSample(false, 5));
    }

    @Test
    public void calcute() {
        // 计算数值类型RDD
        System.err.println(doubleRdd.max());
        System.err.println(doubleRdd.min());
        System.err.println(doubleRdd.sum());
        System.err.println(doubleRdd.mean());
    }

}

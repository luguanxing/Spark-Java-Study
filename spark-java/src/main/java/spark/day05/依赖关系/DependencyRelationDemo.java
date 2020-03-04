package spark.day05.依赖关系;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DependencyRelationDemo {

    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 创建spark相关的context
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("DependencyRelationDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void narrowDependency() {
        // 创建数据
        JavaRDD<Integer> rdd1 = jsc.parallelize(Stream.of(1, 2, 3).collect(Collectors.toList()));
        JavaRDD<String> rdd2 = jsc.parallelize(Stream.of("a", "b").collect(Collectors.toList()));
        // 构造笛卡尔积
        JavaPairRDD<Integer, String> resultRDD = rdd1.cartesian(rdd2);
        // 输出结果
        resultRDD.collect().forEach(System.err::println);
    }

}

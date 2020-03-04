package spark.day06.closure闭包;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BroadcastDemo {

    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 创建spark相关的context
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("BroadcastDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void noBroadcastDemo() {
        // 没有广播变量的情况下map下发到Task，开销比较大
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "hadoop");
        map.put(2, "spark");
        map.put(3, "flink");
        jsc.parallelize(Stream.of(1, 2, 3).collect(Collectors.toList()))
                .map(map::get)
                .collect()
                .forEach(System.err::println);
    }

    @Test
    public void roadcastDemo() {
        // 有广播变量的情况下map下发到Executor
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "hadoop");
        map.put(2, "spark");
        map.put(3, "flink");
        // 使用广播
        Broadcast<Map<Integer, String>> bc = jsc.broadcast(map);
        jsc.parallelize(Stream.of(1, 2, 3).collect(Collectors.toList()))
                .map(bc.getValue()::get)
                .collect()
                .forEach(System.err::println);
    }

}

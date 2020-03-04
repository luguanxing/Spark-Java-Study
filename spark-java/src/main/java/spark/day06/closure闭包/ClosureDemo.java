package spark.day06.closure闭包;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClosureDemo {

    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 创建spark相关的context
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("ClosureDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void closure() {
        // 闭包就是一个对象(可含内部常量)，也是一个函数(用于调用)
        Function<Double, Double> squareFunction = r -> Math.PI * r * r;
        Double square = squareFunction.apply(1.0);
        System.err.println(square);
    }

    @Test
    public void addSumDemo() {
        // 错误的累加demo，因为sum也被分发到了其它executor中，最后仅输出本地的sum=0
        AtomicReference<Integer> sum = new AtomicReference<>(0);
        jsc.parallelize(Stream.of(1, 2, 3 ,4 ,5).collect(Collectors.toList()))
                .foreach(i -> sum.updateAndGet(v -> v + i));
        System.err.println(sum);
    }

}

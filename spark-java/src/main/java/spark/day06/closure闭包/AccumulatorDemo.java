package spark.day06.closure闭包;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AccumulatorDemo {

    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 创建spark相关的context
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AccumulatorDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void accumulator() {
        // 使用全局累加器，能实现全局数值求和
        Accumulator<Integer> accumulator = jsc.accumulator(0);
        jsc.parallelize(Stream.of(1, 2, 3 ,4 ,5).collect(Collectors.toList()))
                .foreach(accumulator::add);
        System.err.println(accumulator.value());
    }

    @Test
    public void UdAccumulator() {
        // 自定义全局累加器
        MyAccumulator myAccumulator = new MyAccumulator();
        JavaSparkContext.toSparkContext(jsc).register(myAccumulator);
        // 累加
        jsc.parallelize(Stream.of(1, 2, 3 ,4 ,5).collect(Collectors.toList()))
                .foreach(myAccumulator::add);
        System.err.println(myAccumulator.value());
    }

}

class MyAccumulator extends AccumulatorV2<Integer, String> {

    private String finalResult = "";

    @Override
    public boolean isZero() {
        // 累加器是否为空
        return "".equals(finalResult);
    }

    @Override
    public AccumulatorV2<Integer, String> copy() {
        // 一个拷贝的累加器
        MyAccumulator newAccumulator = new MyAccumulator();
        newAccumulator.finalResult = finalResult;
        return newAccumulator;
    }

    @Override
    public void reset() {
        // 清空累加器
        finalResult = "";
    }

    @Override
    public void add(Integer integer) {
        // 实现累加
        finalResult += integer + " ";
    }

    @Override
    public void merge(AccumulatorV2<Integer, String> accumulatorV2) {
        // 累加器合并
        finalResult = finalResult + accumulatorV2.value();
    }

    @Override
    public String value() {
        // 返回值
        return finalResult;
    }
}

package spark.day04.checkpoint检查点;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CheckPointDemo {

    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 创建spark相关的context
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("CheckPointDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
        // 需要指定checkpoint位置
        jsc.setCheckpointDir("myCheckpoint");
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void checkPoint() {
        // 设置数据
        JavaPairRDD<Integer, Iterable<Integer>> pairRDD = jsc
                .parallelize(Stream.iterate(0, i -> i + 1).limit(100).collect(Collectors.toList()))
                .mapToPair(i -> new Tuple2<>(i / 10, i))
                .groupByKey();
        JavaRDD<Tuple2<Integer, Iterable<Integer>>> rdd =
                JavaRDD.fromRDD(pairRDD.rdd(), pairRDD.classTag());
        // 避免后续使用时多次计算，可先缓存
        rdd.cache();
        // 使用checkpoint，注意该操作为action算子，会触发上面的计算
        rdd.checkpoint();
        // 后续使用
        JavaRDD<Tuple2<Integer, Iterable<Integer>>> sortRDD1 = rdd.sortBy(tuple2 -> tuple2._1, true, 1);
        JavaRDD<Tuple2<Integer, Iterable<Integer>>> sortRDD2 = rdd.sortBy(tuple2 -> tuple2._1, false, 1);
        // 暂停等待查看
        System.err.println(sortRDD1.first());
        System.err.println(sortRDD2.first());
        try { Thread.sleep(Integer.MAX_VALUE); } catch (Exception e) { e.printStackTrace(); }
    }


}

package spark.day01.Rdd算子;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformationsDemo {

    private SparkConf sparkConf = null;
    private JavaSparkContext jsc = null;
    private JavaRDD<Integer> rdd1 = null;
    private JavaRDD<Integer> rdd2 = null;
    private JavaRDD<String> rdd3 = null;

    @Before
    public void before() {
        // 创建spark相关的context
        sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("TransformationsDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");
        // 模拟数据
        List<Integer> list1 = Stream.iterate(1, i -> i + 1).limit(10).collect(Collectors.toList());
        List<Integer> list2 = Stream.iterate(1, i -> i + 1).limit(3).collect(Collectors.toList());
        List<String> list3 = Stream.of("a", "b", "a").collect(Collectors.toList());
        rdd1 = jsc.parallelize(list1);
        rdd2 = jsc.parallelize(list2);
        rdd3 = jsc.parallelize(list3);
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    @Test
    public void sample() {
        // 按比例随机抽样
        JavaRDD<Integer> sampleRdd = rdd1.sample(false, 0.5);
        sampleRdd.collect().forEach(System.out::println);
    }

    @Test
    public void mapValues() {
        // 对键值对的值进行映射
        rdd1.mapToPair(i -> new Tuple2<>(i, i * 10))
                .mapValues(ii -> "[" + ii + "]")
                .collect().forEach(System.out::println);
    }

    @Test
    public void mapPartitions() {
        // 批处理map整个分区的数据
        rdd1.mapPartitions(
                iterator -> {
                    List<Integer> nums = new ArrayList<>();
                    while (iterator.hasNext()) {
                        Integer num = iterator.next();
                        nums.add(num * num);
                    }
                    return nums.iterator();
                }
        ).collect().forEach(System.out::println);
    }

    @Test
    public void mapPartitionsWithIndex() {
        // 带rdd序号处理map整个分区的数据
        rdd1.mapPartitionsWithIndex(
                (index, iterator) -> {
                    while (iterator.hasNext()) {
                        System.err.println(index + " -> " + iterator.next());
                    }
                    return iterator;
                },
                false
        ).collect();
    }

    @Test
    public void intersection() {
        // 集合交集
        JavaRDD<Integer> intersectionRdd = rdd1.intersection(rdd2);
        intersectionRdd.collect().forEach(System.out::println);
    }

    @Test
    public void union() {
        // 集合并集（可能会重复）
        JavaRDD<Integer> unionRdd = rdd1.union(rdd2);
        unionRdd.collect().forEach(System.out::println);
    }

    @Test
    public void subtract() {
        // 集合差集
        JavaRDD<Integer> subtractRdd = rdd1.subtract(rdd2);
        subtractRdd.collect().forEach(System.out::println);
    }

    @Test
    public void cartesian() {
        // 集合笛卡尔乘积
        JavaPairRDD<Integer, String> cartesianRdd = rdd2.cartesian(rdd3);
        cartesianRdd.foreach(tuple2 -> System.err.println(tuple2));
    }

    @Test
    public void combineByKey() {
        // 模拟数据
        JavaRDD<Tuple2<String, Integer>> nameScoreRdd = jsc.parallelize(
                Stream.of(
                        new Tuple2<>("xiaoming", 90),
                        new Tuple2<>("xiaoming", 95),
                        new Tuple2<>("xiaoming", 100),
                        new Tuple2<>("van", 50),
                        new Tuple2<>("van", 60),
                        new Tuple2<>("spark", 100),
                        new Tuple2<>("spark", 100)
                ).collect(Collectors.toList()));
        JavaPairRDD<String, Integer> nameScorePairRdd = JavaPairRDD.fromJavaRDD(nameScoreRdd);
        // 计算平均分
        JavaPairRDD<String, Tuple2<Integer, Integer>> combineByKeyRdd = nameScorePairRdd
                .combineByKey(
                        // 值转换，90转换得到(90,1)
                        score -> new Tuple2<>(score, 1),
                        // 同分区数据聚合，第一个参数是上面的返回值(90,1)，第二个参数是下一条数据的值95，计算得到聚合结果(185,2)
                        (scoreCount, nextScore) -> new Tuple2<>(scoreCount._1 + nextScore, scoreCount._2 + 1),
                        // 所有数据合并，两个参数都是上面的分区聚合值(185,2)(100,1)，计算得到()285,3
                        (scoreCount1, scoreCount2) -> new Tuple2<>(scoreCount1._1 + scoreCount2._1, scoreCount1._2 + scoreCount2._2)
                );
        // 输出平均分
        combineByKeyRdd.foreach(
                nameScoreCount -> {
                    String name = nameScoreCount._1;
                    Tuple2<Integer, Integer> scoreCount = nameScoreCount._2;
                    Double avgScore = scoreCount._1 * 1.0 / scoreCount._2;
                    System.err.println(name + " -> " + scoreCount + " -> " + avgScore);
                }
        );
    }

    @Test
    public void foldyKey() {
        // 有初始值的reduceByKey
        rdd3.mapToPair(str -> new Tuple2<>(str, 1))
                .foldByKey(10, Integer::sum)
                .foreach(tuple2 -> System.err.println(tuple2));
    }

    @Test
    public void aggregateByKey() {
        // 模拟数据
        JavaRDD<Tuple2<String, Integer>> itemPirceRdd = jsc.parallelize(
                Stream.of(
                        new Tuple2<>("手机", 10),
                        new Tuple2<>("手机", 15),
                        new Tuple2<>("电脑", 20)
                ).collect(Collectors.toList()));
        JavaPairRDD<String, Integer> itemPircePairRdd = JavaPairRDD.fromJavaRDD(itemPirceRdd);
        // 价格打八折求和
        JavaPairRDD<String, Double> itemPirceSumRdd = itemPircePairRdd.aggregateByKey(
                // 初始值
                0.8,
                // 根据初始值对键值对的值运算
                (rate, price) -> rate * price,
                // 对运算结果聚合
                (price1, price2) -> price1 + price2
        );
        // 输出结果
        itemPirceSumRdd.foreach(tuple2 -> System.err.println(tuple2));
    }

    @Test
    public void join() {
        JavaPairRDD<String, Integer> rdd = rdd3.mapToPair(str -> new Tuple2<>(str, 1));
        List<Tuple2<String, Tuple2<Integer, Integer>>> joinRdd = rdd.join(rdd)
                .collect();
        joinRdd.forEach(join -> System.err.println(join));
    }

    @Test
    public void sortBy() {
        // 注意要把JavaPairRDD转成JavaRDD
        JavaPairRDD<String, Integer> pairRDD = rdd3.mapToPair(str -> new Tuple2<>(str, new Random().nextInt(100)));
        JavaRDD<Tuple2<String, Integer>> rdd = JavaRDD.fromRDD(pairRDD.rdd(), pairRDD.classTag());
        rdd.sortBy(tuple2 -> tuple2._1, false, 1)
                .collect().forEach(System.out::println);
    }

    @Test
    public void sortByKey() {
        // JAVA开发不推荐使用sortByKey，尽量使用sortBy
    }

    @Test
    public void sortByValue() {
        JavaPairRDD<String, Integer> pairRdd = rdd3.mapToPair(str -> new Tuple2<>(str, new Random().nextInt(100)));
        // pairRDD转换成RDD，使用sortBy
        JavaRDD<Tuple2<String, Integer>> rdd = JavaRDD.fromRDD(pairRdd.rdd(), pairRdd.classTag());
        rdd.sortBy(strInt -> strInt._2, false, 1)
                .foreach(i -> System.out.println(i));
    }

    @Test
    public void partitions() {
        int before = rdd1.partitions().size();
        int after1 = rdd1.repartition(1).partitions().size();
        int after2 = rdd1.repartition(5).partitions().size();
        System.err.println(before + " -> " + after1);
        System.err.println(before + " -> " + after2);
    }

    @Test
    public void coalesce() {
        int before = rdd1.partitions().size();
        int after1 = rdd1.coalesce(1).partitions().size();
        int after2 = rdd1.coalesce(5, true).partitions().size();    // partitions变多需要指定shuffle
        System.err.println(before + " -> " + after1);
        System.err.println(before + " -> " + after2);
    }

}

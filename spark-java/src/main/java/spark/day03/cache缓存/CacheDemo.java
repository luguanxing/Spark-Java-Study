package spark.day03.cache缓存;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

public class CacheDemo {

    private SparkConf sparkConf = null;
    private JavaSparkContext jsc = null;
    private JavaRDD<Tuple2<String, Integer>> ipCountRdd = null;

    @Before
    public void before() {
        // 创建spark相关的context
        sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("CacheDemo");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");

        // 从文件读入行rdd
        String filePath = "src/main/resources/access_log_sample.txt";
        JavaRDD<String> lineRdd = jsc.textFile(filePath);

        // 抽取出IP并清洗数据
        JavaRDD<String> ipRdd = lineRdd
                .map(line -> line.split(" ")[0])
                .filter(StringUtils::isNotEmpty);

        // 计算ip出现次数
        JavaPairRDD<String, Integer> ipCountPairRdd = ipRdd
                .mapToPair(ip -> new Tuple2<>(ip, 1))
                .reduceByKey(Integer::sum);
        ipCountRdd = JavaRDD.fromRDD(ipCountPairRdd.rdd(), ipCountPairRdd.classTag());
    }

    @After
    public void after() {
        // 关闭资源
        jsc.stop();
    }

    public void countIp(JavaRDD<Tuple2<String, Integer>> ipCountRdd) {
        // 统计出现次数最多和最少的ip
        Tuple2<String, Integer> maxCountIp = ipCountRdd.sortBy(ipCount -> ipCount._2, false, ipCountRdd.partitions().size()).first();
        Tuple2<String, Integer> minCountIp = ipCountRdd.sortBy(ipCount -> ipCount._2, true, ipCountRdd.partitions().size()).first();

        // 查看结果
        System.err.println("maxCountIp = " + maxCountIp);
        System.err.println("minCountIp = " + minCountIp);
        try { Thread.sleep(Integer.MAX_VALUE); } catch (Exception e) {e.printStackTrace();}
    }

    @Test
    public void test() {
        countIp(ipCountRdd);
    }

    @Test
    public void cache() {
        // 使用缓存
        ipCountRdd.cache();
        countIp(ipCountRdd);
    }

    @Test
    public void persist() {
        // 指定持久化存储级别
        ipCountRdd.persist(StorageLevel.MEMORY_ONLY());
        // 查看缓存级别
        StorageLevel storageLevel = ipCountRdd.getStorageLevel();
        System.err.println(storageLevel);
    }

}

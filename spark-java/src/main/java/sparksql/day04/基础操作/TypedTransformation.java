package sparksql.day04.基础操作;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.Student;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TypedTransformation {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;
    Dataset<String> stringDS = null;
    Dataset<Row> studentDF = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("TypedTransformation")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 准备数据
        stringDS = sparkSession.createDataset(Stream.of("hello spark", "hello hadoop").collect(Collectors.toList()), Encoders.STRING());
        // 使用类创建数据行的Schema
        StructType studentSchema = new StructType()
                .add("name", "string")
                .add("age", "integer")
                .add("gpa", "float");
        // 读文件数据并转换dataframe
        String filePath = "src/main/resources/studenttab10k";
        studentDF = sparkSession
                .read()
                .option("delimiter", "\t")
                .schema(studentSchema)
                .csv(filePath);
    }

    @After
    public void after() {
        jsc.stop();
        sparkSession.close();
    }

    @Test
    public void transformation() {
        // flatMap
        stringDS.flatMap(str -> Arrays.asList(str.split(" ")).iterator(), Encoders.STRING()).show();
        // map
        stringDS.map(str -> "[" + str + "]", Encoders.STRING()).show();
        // mapPartitions
        stringDS.mapPartitions(
                // 每个分区的数据要求能放到内存中，否则出现OOM
                partitionIterator -> {
                    List<String> newList = new ArrayList<>();
                    partitionIterator.forEachRemaining(str -> newList.add("<" + str + ">"));
                    return newList.iterator();
                },
                Encoders.STRING()
        ).show();
        // filter
        sparkSession
                .range(10)
                .filter(num -> num > 5)
                .show();
    }

    @Test
    public void as() {
        // 使用as将dataframe转dataframe
        Dataset<Student> stduentDS = studentDF.as(Encoders.bean(Student.class));
        stduentDS.show();
    }

    @Test
    public void groupByKey() {
        Dataset<String> wordDS = stringDS.flatMap(str -> Arrays.asList(str.split(" ")).iterator(), Encoders.STRING());
        // 根据key分组，结果需要聚合才返回dataset
        KeyValueGroupedDataset<String, String> groupKV = wordDS.groupByKey(word -> word, Encoders.STRING());
        groupKV.count().show();
        groupKV.keys().show();
    }

    @Test
    public void sample() {
        Dataset<Long> ds = sparkSession.range(15);
        // 根据权重划分，权重越高划分得到的元素较多的几率就越大
        double[] weights = {2d, 1d, 1d};
        Stream.of(ds.randomSplit(weights)).forEach(Dataset::show);
        // 根据概率采样
        ds.sample(false, 0.5).show();
    }

    @Test
    public void sort() {
        Dataset<Student> stduentDS = studentDF.as(Encoders.bean(Student.class));
        // orderBy排序，可指定升序或降序
        stduentDS.orderBy(functions.desc("gpa")).show();
        // sort排序，方法和orderBy一样
        stduentDS.sort(functions.asc("age")).show();
    }

    @Test
    public void deduplicate() {
        // distinct全部属性去重
        studentDF.distinct().show();
        // dropDuplicates根据某属性去重
        studentDF.dropDuplicates("gpa").orderBy(functions.desc("gpa")).show();
    }

    @Test
    public void collection() {
        Dataset<Long> ds1 = sparkSession.range(1, 10);
        Dataset<Long> ds2 = sparkSession.range(5, 15);
        // 差集
        ds1.except(ds2).show();
        // 交集
        ds1.intersect(ds2).show();
        // 并集
        ds1.union(ds2).show();
        // limit
        ds1.limit(3).show();
    }

}
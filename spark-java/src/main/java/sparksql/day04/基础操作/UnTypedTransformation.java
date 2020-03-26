package sparksql.day04.基础操作;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.Person;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class UnTypedTransformation {


    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;
    Dataset<Person> personDS = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("TypedTransformation")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 创建数据集
        List<Person> personList = Stream.of(
                new Person("van", 30),
                new Person("zhangsan", 18),
                new Person("zhangsan", 28)
        ).collect(Collectors.toList());
        personDS = sparkSession.createDataset(personList, Encoders.bean(Person.class));
    }

    @After
    public void after() {
        jsc.stop();
        sparkSession.close();
    }

    @Test
    public void select() {
        // 选择已有列
        personDS.select("name").show();
        personDS.selectExpr("COUNT(age) AS count", "SUM(age) AS sum", "AVG(age) AS avg").show();
        personDS.select(expr("COUNT(age) AS count"), expr("SUM(age) AS sum"), expr("AVG(age) AS avg")).show();
    }

    @Test
    public void column() {
        // 添加新函数列
        personDS.withColumn("random", rand()).show();
        // 添加自定义函数
        sparkSession.udf().register("myFunction", (name, age) -> name + "->" + age, DataTypes.StringType);
        personDS.withColumn("myColumn", callUDF("myFunction", personDS.col("name"), personDS.col("age"))).show();
        // 重命名列
        personDS.withColumnRenamed("name", "name_new").show();
        // 去掉列
        personDS.drop("age").show();
    }

    @Test
    public void groupBy() {
        // groupByKey有类型，即在算子中有具体对象类型
        personDS
                .groupByKey(Person::getName, Encoders.STRING()) // 注意类对象需要无参构造器
                .count()
                .show();
        // groupBy无类型，即在算子中无对象类型，仅需要指定列
        personDS
                .groupBy("name")
                .agg(sum("age"))
                .show();
    }

}

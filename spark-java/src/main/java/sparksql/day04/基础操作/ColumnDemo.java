package sparksql.day04.基础操作;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.Person;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ColumnDemo {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;
    private Dataset<Person> personDS = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("ColumnDemo")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 创建数据集
        List<Person> personList = Stream.of(
                new Person("van", 30),
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
    public void createColumn() {
        // 使用col创建列创建列，无需依赖
        Column column1 = functions.col("name");
        // 使用columns方法创建列，无需依赖
        Column column2 = functions.column("name");
        // 使用dataframe或dataset创建的列，但这些列存在了对应绑定以便在join等操作时使用
        Column column3 = personDS.col("name");
        Column column4 = personDS.toDF().col("name");
        // 使用dataframe或dataset创建的列
        Column column5 = personDS.apply("name");
        // select中使用列
        personDS.select(column1, column2, column3, column4, column5).show();
    }

    @Test
    public void transformColumn() {
        // 创建列别名
        personDS.select(functions.col("name").as("name_new")).show();
        // 列类型转换
        personDS.printSchema();
        personDS.select(functions.col("age").cast(DataTypes.StringType), functions.col("name")).printSchema();
    }

    @Test
    public void useColumn() {
        // 增加列：双倍年龄
        personDS.withColumn("doubled", functions.col("age").multiply(2)).show();
        // 模糊查询
        personDS.where(functions.col("name").like("%v%")).show();
        // 排序：正反序
        personDS.sort(functions.asc("age")).show();
        personDS.sort(functions.desc("age")).show();
        // 枚举判断
        personDS.where(functions.col("name").isin("van", "vandarkholme")).show();
        // 列关联
        personDS.join(personDS, personDS.col("name").equalTo(personDS.col("name"))).show();
    }

}

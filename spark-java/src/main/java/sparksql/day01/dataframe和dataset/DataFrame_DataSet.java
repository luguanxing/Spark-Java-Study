package sparksql.day01.dataframe和dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.Person;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataFrame_DataSet {

    SparkSession sparkSession = null;
    JavaSparkContext jsc = null;
    JavaRDD<Person> personRDD = null;

    @Before
    public void before() {
        // 初始化数据
        sparkSession = SparkSession.builder()
                .appName("DataFrameDataSet")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        personRDD = jsc.parallelize(Stream.of(new Person("van", 30), new Person("xiaoming", 20)).collect(Collectors.toList()));
    }

    @After
    public void after() {
        jsc.close();
        sparkSession.close();
    }

    @Test
    public void difference() {
        // 对比DataFrame和DataSet区别
        Dataset<Row> personDF = sparkSession.createDataFrame(personRDD, Person.class);
        Dataset<Person> personDS = sparkSession.createDataset(personRDD.rdd(), Encoders.bean(Person.class));
        // 操作map对比：DataFrame中所存放的是Row弱对象,而Dataset中可以存放任何类型的强对象
        personDF
                .map(
                        row -> RowFactory.create(row.getInt(0) * 2, row.get(1)),
                        RowEncoder.apply(personDF.schema())
                ).show();
        personDS
                .map(
                        person -> {
                            person.setAge(person.getAge() * 2);
                            return person;
                        },
                        Encoders.bean(Person.class)
                )
                .show();
        // 编译时安全：DataFrame只能做到运行时类型检查, Dataset能做到编译和运行时都有类型检查
        personDF.groupBy("age");
        personDS.filter(person -> person.getAge() > 18);
    }

    @Test
    public void Df2Ds() {
        // DataFrame和DataSet相互转换
        Dataset<Row> personDF = sparkSession.createDataFrame(personRDD, Person.class);
        Dataset<Person> personDS = sparkSession.createDataset(personRDD.rdd(), Encoders.bean(Person.class));
        // DataFrame转DataSet，需要Bean类型
        Dataset<Person> df2ds = personDF.as(Encoders.bean(Person.class));
        df2ds.show();
        // DataSet转DataFrame
        Dataset<Row> ds2df = personDS.toDF();
        ds2df.show();
    }

}

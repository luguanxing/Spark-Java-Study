package sparksql.day01.dataframe和dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.Person;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatasetDemo {

    SparkSession sparkSession = null;
    JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 初始化数据
        sparkSession = SparkSession.builder()
                .appName("DatasetDemo")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        jsc.close();
        sparkSession.close();
    }

    @Test
    public void createDataSet() {
        // 基本类型数据
        List<Person> personList = Stream.of(new Person("van", 30), new Person("xiaoming", 20)).collect(Collectors.toList());
        JavaRDD<Person> personRDD = jsc.parallelize(personList);
        Dataset<Person> personDS = sparkSession.createDataset(personRDD.rdd(), Encoders.bean(Person.class));
        personDS.show();
    }

    @Test
    public void dataSetType() {
        // Dataset底层执行的RDD是InternalRow
        List<Person> personList = Stream.of(new Person("van", 30)).collect(Collectors.toList());
        JavaRDD<Person> personRDD = jsc.parallelize(personList);
        Dataset<Person> personDS = sparkSession.createDataset(personRDD.rdd(), Encoders.bean(Person.class));
        RDD<InternalRow> internalRowRDD = personDS.queryExecution().toRdd();
    }

}

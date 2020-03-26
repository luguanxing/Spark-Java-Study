package sparksql.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import sparksql.common.Person;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DsDfDemo {

    @Test
    public void DsDf() {
        // 初始化数据
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataframeDatasetDemo")
                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        JavaRDD<Person> personRDD = jsc.parallelize(Stream.of(new Person("van", 30), new Person("xiaoming", 20)).collect(Collectors.toList()));
        // 转换成dataframe或dataset
        Dataset<Row> personDF = sparkSession.createDataFrame(personRDD, Person.class);
        Dataset<Person> personDS = personDF.as(Encoders.bean(Person.class));
        personDF.show();
        personDS.show();
        // 使用sql查询
        personDF.createOrReplaceTempView("tbl_person");
        sparkSession.sql("SELECT * FROM tbl_person WHERE age = 20").show();
    }


}

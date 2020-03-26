package sparksql.day01.dataframe和dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.Person;
import sparksql.common.PmData;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataFrameDemo {

    SparkSession sparkSession = null;
    JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 初始化数据
        sparkSession = SparkSession.builder()
                .appName("DataFrameDemo")
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
    public void createDataframe() {
        // 创建dataframe
        List<Person> personList = Stream.of(new Person("van", 30), new Person("xiaoming", 20)).collect(Collectors.toList());
        JavaRDD<Person> personRDD = jsc.parallelize(personList);
        Dataset<Row> personDF1 = sparkSession.createDataFrame(personRDD, Person.class);
        Dataset<Row> personDF2 = sparkSession.createDataFrame(personList, Person.class);
        Dataset<Row> pmDF = sparkSession.read().csv("src/main/resources/BeijingPM20100101_20151231.csv");
        personDF1.show();
        personDF2.show();
        pmDF.show();
    }

    @Test
    public void inferSchema() {
        // 根据bean生成schema
        StructType pmDataSchema = Encoders.bean(PmData.class).schema();
        // 读取数据并使用schema
        Dataset<Row> pmDF = sparkSession
                .read()
                .option("header", true)
                .schema(pmDataSchema)
                .csv("src/main/resources/beijingpm_with_nan.csv");
        pmDF.show();
    }

    @Test
    public void analysisExample() {
        // 读入数据
        String dataPath = "src/main/resources/BeijingPM20100101_20151231.csv";
        Dataset<Row> pmDF = sparkSession
                .read()
                .option("header", true)
                .csv(dataPath);
        // 声明式SQL处理数据
        String sql = "SELECT year, month, COUNT(1) AS cnt FROM tbl_pm25 WHERE PM_Dongsi != \"NA\" GROUP BY year, month ORDER BY cnt DESC";
        pmDF.createOrReplaceTempView("tbl_pm25");
        sparkSession
                .sql(sql)
                .show();
        // 命令式处理数据
        pmDF.select("year", "month", "PM_Dongsi")
                .where("PM_Dongsi != \"NA\"")
                .groupBy("year", "month")
                .count()
                .orderBy(functions.desc("count"))
                .show();
    }

}

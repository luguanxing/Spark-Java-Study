package sparksql.day05.常规操作;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class NullProcessDemo {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("NullProcessDemo")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
    }

    @After
    public void after() {
        jsc.stop();
        sparkSession.close();
    }

    @Test
    public void nullAndNaN() {
        // 读取数据集对应类型：
        // (1)使用自动推断会将NaN推断为字符串，要手动map转换
        // (2)可指定Schema不推断
        StructType pmDataSchema = new StructType()
                .add("id", "long")
                .add("year", "integer")
                .add("month", "integer")
                .add("day", "integer")
                .add("hour", "integer")
                .add("season", "integer")
                .add("pm", "double");
        Dataset<Row> pmDF = sparkSession
                .read()
                .option("header", true)
                .schema(pmDataSchema)
                .csv("src/main/resources/beijingpm_with_nan.csv");
        // 只要某列为null就丢弃整行的数据
        pmDF.na().drop("any").show();
        pmDF.na().drop().show();
        // 全列为null才丢弃
        pmDF.na().drop("all").show();
        // 某些列规则才丢弃
        pmDF.na().drop("any", new String[]{"year", "month", "day", "hour"}).show();
        // 填充所有null列
        pmDF.na().fill(0).show();
        // 填充某些列
        pmDF.na().fill(0, new String[]{"year", "month", "day", "hour"}).show();
    }

    @Test
    public void nullStr() {
        // 读取数据集
        Dataset<Row> pmDF = sparkSession
                .read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/BeijingPM20100101_20151231.csv");
        // 丢弃空字符数据
        pmDF.where("PM_Dongsi != \"NA\"").show();
        pmDF.filter("PM_Dongsi != \"NA\"").show();
        // 替换空字符数据
        pmDF
                .select("No", "year", "month", "day", "hour", "PM_Dongsi")
                .withColumn(
                        "PM_Dongsi_All", functions
                                .when(pmDF.col("PM_Dongsi").equalTo("NA"), -1d)
                                .otherwise(pmDF.col("PM_Dongsi").as(Encoders.DOUBLE()))
                )
                .show();
        // 使用映射map
        Map repalceMap = new HashMap<String, String>();
        repalceMap.put("NA", "null");
        pmDF
                .na()
                .replace("PM_Dongsi", repalceMap)
                .show();
    }

}

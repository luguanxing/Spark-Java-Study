package sparksql.day05.常规操作;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.City;
import sparksql.common.People;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;

public class JoinProcessDemo {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;
    private Dataset<People> personDS = null;
    private Dataset<City> cityDS = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("JoinProcessDemo")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 创建数据集
        List<People> personList = new ArrayList<>();
        personList.add(new People(0, "Lucy", 0));
        personList.add(new People(1, "Lily", 0));
        personList.add(new People(2, "Van", 1));
        personList.add(new People(3, "Kim", 3));
        personDS = sparkSession.createDataset(personList, Encoders.bean(People.class));
        List<City> cityList = new ArrayList<>();
        cityList.add(new City(0, "Beijing"));
        cityList.add(new City(1, "Shanghai"));
        cityList.add(new City(2, "Guangzhou"));
        cityDS = sparkSession.createDataset(cityList, Encoders.bean(City.class));
    }

    @After
    public void after() {
        jsc.stop();
        sparkSession.close();
    }

    @Test
    public void cross() {
        // 全连接(笛卡尔积)，一般需要使用where过滤
        personDS
                .crossJoin(cityDS)
                .where(personDS.col("cityId").equalTo(cityDS.col("id")))
                .select(personDS.col("id"), personDS.col("name"), cityDS.col("name").as("city"))
                .show();
    }

    @Test
    public void join() {
        // 一般连接方式
        personDS
                .join(cityDS, personDS.col("cityId").equalTo(cityDS.col("id")))
                .select(personDS.col("id"), personDS.col("name"), cityDS.col("name").as("city"))
                .show();
        // 内连接，只连接能连接的数据
        personDS
                .join(cityDS, personDS.col("cityId").equalTo(cityDS.col("id")), "inner")
                .select(personDS.col("id"), personDS.col("name"), cityDS.col("name").as("city"))
                .show();
        // 全外连接，没有连接上的数据也会显示出来
        personDS
                .join(cityDS, personDS.col("cityId").equalTo(cityDS.col("id")), "full")
                .select(personDS.col("id"), personDS.col("name"), cityDS.col("name").as("city"))
                .show();
        // 左外连接，左边没有连接上的数据也会显示
        personDS
                .join(cityDS, personDS.col("cityId").equalTo(cityDS.col("id")), "left")
                .select(personDS.col("id"), personDS.col("name"), cityDS.col("name").as("city"))
                .show();
        // 右外连接，右边没有连接上的数据也会显示
        personDS
                .join(cityDS, personDS.col("cityId").equalTo(cityDS.col("id")), "right")
                .select(personDS.col("id"), personDS.col("name"), cityDS.col("name").as("city"))
                .show();
        // leftAnti只显示左侧未成功连接的数据，只能显示左侧数据及其列
        personDS
                .join(cityDS, personDS.col("cityId").equalTo(cityDS.col("id")), "leftanti")
                .select(personDS.col("id"), personDS.col("name"))
                .show();
        // leftSemi只显示左侧成功连接的数据，只能显示左侧数据及其列
        personDS
                .join(cityDS, personDS.col("cityId").equalTo(cityDS.col("id")), "leftsemi")
                .select(personDS.col("id"), personDS.col("name"))
                .show();
    }

    @Test
    public void udf() {
        // 自定义函数
        sparkSession.udf().register("myFunction1", (name) -> "[" + name + "]", DataTypes.StringType);
        sparkSession.udf().register("myFunction2", myFunction2, DataTypes.StringType);
        personDS
                .withColumn("name1", callUDF("myFunction1", personDS.col("name")))
                .withColumn("name2", callUDF("myFunction2", personDS.col("name")))
                .show();
    }

    private static UDF1 myFunction2 = (UDF1<String, String>) name -> {
        // 自定义函数内部
        String newName = "<" + name + ">";
        return newName;
    };

}

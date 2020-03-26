package sparksql.day02.数据读写;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReadWriteDemo {

    SparkSession sparkSession = null;
    JavaSparkContext jsc = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("ReadWriteDemo")
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
    public void readDemo() {
        // 使用load方式读文件
        String filePath = "src/main/resources/BeijingPM20100101_20151231.csv";
        sparkSession.read()
                .format("csv")
                .option("header", true)         // 数据带头部信息
                .option("inferSchema", true)    // 类型自动推断
                .load(filePath)
                .show();
        // 使用封装方式读文件
        sparkSession.read()
                .option("header", true)         // 数据带头部信息
                .option("inferSchema", true)    // 类型自动推断
                .csv(filePath)
                .show();
    }

    @Test
    public void writeDemo() {
        // 输入输出数据源
        String inputPath = "src/main/resources/BeijingPM20100101_20151231.csv";
        String ouputPath = "target/output/BeijingPM20100101_20151231.json";
        // 读入文件
        Dataset<Row> pmDF = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath);
        // 输出文件
        pmDF.write().mode(SaveMode.Overwrite).json(ouputPath);
    }

}

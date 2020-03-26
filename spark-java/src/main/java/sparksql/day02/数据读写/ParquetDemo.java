package sparksql.day02.数据读写;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParquetDemo {

    SparkSession sparkSession = null;
    JavaSparkContext jsc = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("ParquetDemo")
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
    public void writeParquet() {
        // 写parquet格式
        String inputPath = "src/main/resources/BeijingPM20100101_20151231.csv";
        String ouputPath = "target/output/BeijingPM20100101_20151231.parquet";
        // 读入文件
        Dataset<Row> pmDF = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath)
                .select("year", "month", "day");
        // 输出文件
        pmDF.write().mode(SaveMode.Overwrite).parquet(ouputPath);
    }

    @Test
    public void writePartition() {
        // 写parquet格式
        String inputPath = "src/main/resources/BeijingPM20100101_20151231.csv";
        String ouputPath = "target/output/BeijingPM20100101_20151231.partition.parquet";
        // 读入文件
        Dataset<Row> pmDF = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath)
                .select("year", "month", "day");
        // 按分区写，分区列不会被包含
        pmDF.write().mode(SaveMode.Overwrite).partitionBy("year", "month").save(ouputPath);
    }

    @Test
    public void readPartition() {
        // 写parquet格式
        String inputPath = "target/output/BeijingPM20100101_20151231.partition.parquet";
        String inputPath_partition = "target/output/BeijingPM20100101_20151231.partition.parquet/year=2010/month=1";
        // 读入某特定分区，会自动读取分区
        Dataset<Row> pmDF = sparkSession.read().parquet(inputPath);
        Dataset<Row> pmDF_partition = sparkSession.read().parquet(inputPath_partition);
        pmDF.printSchema();
        pmDF_partition.printSchema();
        // 分区内数据会没有分区字段
        pmDF.show();
        pmDF_partition.show();
    }

}

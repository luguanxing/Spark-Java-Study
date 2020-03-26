package sparksql.day05.常规操作;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.PmData;

public class AggProcessDemo {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;
    private Dataset<Row> pmDF = null;
    private Dataset<Row> pmSource = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("AggProcessDemo")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 读入数据集，过滤掉无值的数据
        pmDF = sparkSession
                .read()
                .option("header", true)
                .csv("src/main/resources/beijingpm_with_nan.csv")
                .as(Encoders.bean(PmData.class))
                .where("pm != \"NaN\"")
                .select(functions.col("year"), functions.col("month"), functions.col("pm").cast(DataTypes.DoubleType));
        // 读入多维数据集
        pmSource = sparkSession
                .read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/pm_final.csv");

    }

    @After
    public void after() {
        jsc.stop();
        sparkSession.close();
    }

    @Test
    public void groupBy() {
        // 需求：计算每个月PM的平均数，需要分组聚合
        // 获取分组数据集
        RelationalGroupedDataset relationalGroupedDataset = pmDF.groupBy("year", "month");
        // 对分组数据集进行聚合
        relationalGroupedDataset
                .agg(functions.avg("pm").as("pm_avg"))
                .orderBy(functions.desc("pm_avg"))
                .show();
        relationalGroupedDataset
                .avg("pm")
                .withColumnRenamed("avg(pm)", "pm_avg")
                .orderBy(functions.desc("pm_avg"))
                .show();
    }

    @Test
    public void multiAgg() {
        // 按来源和年分组，聚合PM平均值
        Dataset<Row> pm_year_source = pmSource
                .groupBy("source", "year")
                .avg("pm")
                .withColumnRenamed("avg(pm)", "pm_avg");
        // 按来源分组，聚合PM平均值
        Dataset<Row> pm_source = pmSource
                .groupBy("source")
                .avg("pm")
                .withColumnRenamed("avg(pm)", "pm_avg");
        //合并结果，汇聚了多个维度的小计
        pm_source
                .select(functions.col("source"), functions.lit(null).as("year"), functions.col("pm_avg"))
                .union(pm_year_source)
                .sort(functions.col("source"), functions.asc_nulls_last("year"), functions.col("pm_avg"))
                .show();
    }

    @Test
    public void rollup() {
        // 滚动分组：从右到左逐个减少列聚合，即聚合数量=列数+1(全局聚合)
        pmSource
                .rollup("source", "year")
                .avg("pm").withColumnRenamed("avg(pm)", "pm_avg")
                .sort(functions.desc_nulls_last("source"), functions.desc_nulls_last("year"))
                .show();
    }

    @Test
    public void cube() {
        // 全维度分组
        pmSource
                .cube("source", "year")
                .avg("pm").withColumnRenamed("avg(pm)", "pm_avg")
                .sort(functions.desc_nulls_last("source"), functions.desc_nulls_last("year"))
                .show();

    }

    @Test
    public void sparkSqlCube(){
        // 使用sql实现全维度分组
        pmSource.createOrReplaceTempView("pm_source");
        sparkSession
                .sql(
                "SELECT source, year, avg(pm) AS pm_avg " +
                        "FROM pm_source " +
                        "GROUP BY source, year " +
                        "GROUPING SETS ((source, year), (source), (year), ()) " +
                        "ORDER BY source DESC NULLS LAST, year  DESC NULLS LAST")
                .show();
    }

}

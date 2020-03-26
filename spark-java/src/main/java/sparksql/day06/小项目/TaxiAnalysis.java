package sparksql.day06.小项目;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.google.gson.Gson;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.io.Codec;
import scala.io.Source;
import sparksql.common.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class TaxiAnalysis {

    private static SparkSession sparkSession = null;
    private static JavaSparkContext jsc = null;
    private static Dataset<Row> tripDS = null;
    private static Broadcast<FeatureCollection> broadcast = null;

    @Before
    public void before() {
        // 创建SparkSession
        sparkSession = SparkSession
                .builder()
                .appName("TaxiAnalysis")
                .master("local[*]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 读取数据集
        String inputFile = "src/main/resources/half_trip.csv";
        Dataset<Row> dataDF = sparkSession
                .read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(inputFile);
        // 数据集处理转换
        JavaRDD<Trip> tripRDD = dataDF
                .javaRDD()
                .map(Trip::parseRowToTrip)
                .filter(Objects::nonNull);
        tripDS = sparkSession
                .createDataFrame(tripRDD, Trip.class);
    }


    @After
    public void after() {
        jsc.stop();
        sparkSession.close();
    }

    @Test
    public void getTripCostTimeStat() {
        // 计算每趟的时长分布，按N分钟区间统计，方便查看分布和反常值
        int timeDuration = 5;
        sparkSession
                .udf()
                .register("getTripCostMinutes", (t2, t1) -> ((Long) t2 - (Long) t1) / (60 * 1000), DataTypes.LongType);
        Dataset<Row> durationStat = tripDS
                .withColumn("tripCostTime", functions.callUDF("getTripCostMinutes", functions.col("dropOffTs"), functions.col("pickUpTs")))
                .groupBy(functions.round(functions.col("tripCostTime").divide(timeDuration)).as("tripCostTime"))
                .count()
                .orderBy(functions.asc("tripCostTime"))
                .withColumn("rangeStart", functions.col("tripCostTime").multiply(timeDuration))
                .withColumn("rangeEnd", functions.col("tripCostTime").plus(1).multiply(timeDuration))
                .drop("tripCostTime")
                .select("rangeStart", "rangeEnd", "count");
        durationStat.show();
    }

    @Test
    public void getBoroughStat() {
        // 增加行政区信息
        // 1.读取行政区数据集并按面积排序
        String json = Source.fromFile("src/main/resources/nyc-borough-boundaries-polygon.geojson.txt", Codec.UTF8()).mkString();
        FeatureCollection featureCollection = new Gson().fromJson(json, FeatureCollection.class);
        featureCollection.getFeatures().sort(Comparator.comparingDouble(f -> -f.getGeometry().calculateArea2D()));
        // 2.广播政区信息
        broadcast = jsc.broadcast(featureCollection);
        // 3.创建UDF实现定位查找行政区
        sparkSession.udf().register("getBoroughName", getBoroughName, DataTypes.StringType);
        // 4.数据集添加行政区信息，并过滤找不到行政区的数据行
        Dataset<Row> tripWithBoroughDS = tripDS
                .withColumn("pickUpBorough", functions.callUDF("getBoroughName", functions.col("pickUpY"), functions.col("pickUpX")))
                .withColumn("dropBorough", functions.callUDF("getBoroughName", functions.col("dropOffY"), functions.col("dropOffX")))
                .filter(functions.col("pickUpBorough").notEqual("NoMatchBorough").or(functions.col("pickUpBorough").notEqual("Unknown Borough")))
                .filter(functions.col("dropBorough").notEqual("NoMatchBorough").or(functions.col("dropBorough").notEqual("Unknown Borough")));
        // 会话统计
        // 1.转换为统计类
        JavaRDD<BoroughStat> boroughStatRDD = tripWithBoroughDS
                .javaRDD()
                .map(BoroughStat::parseRowToBoroughStat);
        Dataset<BoroughStat> boroughStatDS = sparkSession
                .createDataset(boroughStatRDD.rdd(), Encoders.bean(BoroughStat.class));
        // 2.根据司机分组所有数据，并计算统计结果
        Dataset<BoroughStatResult> statResultDS = boroughStatDS
                .groupByKey(BoroughStat::getLicense, Encoders.STRING())
                .flatMapGroups(
                        (license, boroughStats) -> {
                            // 获取该司机所有数据
                            List<BoroughStat> boroughStatList = new ArrayList<>();
                            while (boroughStats.hasNext()) {
                                boroughStatList.add(boroughStats.next());
                            }
                            // 对该司机所有数据按时间排序
                            boroughStatList.sort(Comparator.comparingLong(BoroughStat::getDropOffTs));
                            // 取每两个符合条件的相邻数据计算得到统计
                            List<BoroughStatResult> boroughStatResultList = new ArrayList<>();
                            for (int i = 0; i < boroughStatList.size() / 2; i++) {
                                if (2 * i + 1 < boroughStatList.size()) {
                                    BoroughStat boroughStat1 = boroughStatList.get(2 * i);
                                    BoroughStat boroughStat2 = boroughStatList.get(2 * i + 1);
                                    if (boroughStat1.getDropBorough().equals(boroughStat2.getDropBorough())) {
                                        // 条件：相同区域内上个客人下车到下个客人上车时间
                                        String borough = boroughStat1.getDropBorough();
                                        Long dropOffTs = boroughStat1.getDropOffTs();
                                        Long pickUpTs = boroughStat2.getPickUpTs();
                                        if (pickUpTs > dropOffTs) {
                                            boroughStatResultList.add(new BoroughStatResult(license, borough, pickUpTs - dropOffTs));
                                        }
                                    }
                                }
                            }
                            return boroughStatResultList.iterator();
                        },
                        Encoders.bean(BoroughStatResult.class)
                );
        // 3.根据统计对行政区聚合计算平均时间
        statResultDS
                .groupBy("borough")
                .avg("costTime")
                .show();
    }

    private static UDF2 getBoroughName = (UDF2<Double, Double, String>) (y, x) -> {
        Feature matchFeature = broadcast
                .getValue()
                .getFeatures()
                .stream()
                .filter(
                        feature -> {
                            Geometry geometry = feature.getGeometry();
                            return GeometryEngine.contains(geometry, new Point((Double) y, (Double) x), SpatialReference.create(4326));
                        }
                )
                .findFirst()
                .orElse(null);
        if (matchFeature != null) {
            return matchFeature.getBoroughName();
        } else {
            return "NoMatchBorough";
        }
    };

}

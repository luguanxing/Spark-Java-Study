package sparksql.day05.常规操作;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sparksql.common.Product;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WindowDemo {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;
    private Dataset<Product> productDS = null;

    @Before
    public void before() {
        sparkSession = SparkSession.builder()
                .appName("WindowDemo")
                .master("local[2]")
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("ERROR");
        // 准备数据集
        List<Product> productList = Stream.of(
                new Product("P30", "phone", 5000),
                new Product("P40", "phone", 6000),
                new Product("iPhone X", "phone", 10000),
                new Product("MateX Pro", "pc", 6000),
                new Product("macbook", "pc", 12000),
                new Product("Hasee", "pc", 9000),
                new Product("mein kampf", "book", 500),
                new Product("java", "book", 500),
                new Product("python", "book", 500),
                new Product("cs1.6", "game", 100),
                new Product("macbook pro", "pc", 20000)
        ).collect(Collectors.toList());
        productDS = sparkSession.createDataset(productList, Encoders.bean(Product.class));
    }

    @After
    public void after() {
        jsc.stop();
        sparkSession.close();
    }

    @Test
    public void windowLimit() {
        // 定义分组窗口和窗口内排序方式
        WindowSpec window = Window
                .partitionBy("category")
                .orderBy(functions.desc("price"));
        // 利用窗口计算每个分组数据排序的前两名排名
        productDS
                .withColumn("rank", functions.dense_rank().over(window))
                .where("rank <= 2")
                .show();
    }

    @Test
    public void windowLimitSQL() {
        // 使用sql计算分组topN
        productDS.createOrReplaceTempView("tbl_product");
        String subSql = "SELECT *, dense_rank() OVER (PARTITION BY category ORDER BY price DESC) AS rank FROM tbl_product";
        String sql = "SELECT name, category, price FROM (" + subSql + ") WHERE rank <= 2";
        sparkSession
                .sql(sql)
                .show();
    }

    @Test
    public void windowDifference() {
        // 定义最分组内值差的窗口
        WindowSpec diffWindow = Window
                .partitionBy("category")
                .orderBy(functions.desc("price"));
        // 定义最分组内值差排名窗口
        WindowSpec rankWindow = Window
                .partitionBy("category")
                .orderBy(functions.desc("differenceToMax"));
        // 利用窗口计算每个分组数据最大差值，并选出每个分组的前两名
        productDS
                .withColumn("differenceToMax", (functions.max("price").over(diffWindow)).minus(functions.col("price")))
                .withColumn("rank", functions.dense_rank().over(rankWindow))
                .where("rank <= 2")
                .orderBy(functions.desc("category"), functions.desc("differenceToMax"))
                .show();
    }

    @Test
    public void windowDifferenceSQL() {
        // 使用sql计算分组内与最大值差距最大的前两名
        productDS.createOrReplaceTempView("tbl_product");
        String subSubSql = "SELECT *, (max(price) OVER (PARTITION BY category ORDER BY price DESC) - price) AS differenceToMax FROM tbl_product";
        String subSql = "SELECT *, dense_rank() OVER (PARTITION BY category ORDER BY differenceToMax DESC) AS rank FROM (" + subSubSql + ")";
        String sql = "SELECT name, category, price, differenceToMax FROM (" + subSql + ") WHERE rank <= 2 ORDER BY category DESC, differenceToMax DESC";
        sparkSession
                .sql(sql)
                .show();
    }

}

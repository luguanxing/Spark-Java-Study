package sparksql.day03.整合数据源;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HiveDemo {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 初始化数据，连接metastore获取元信息
        sparkSession = SparkSession.builder()
                .appName("HiveDemo")
                .master("local[2]")
                .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
                .enableHiveSupport()
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
    public void readData() {
        sparkSession.sql("SHOW DATABASES").show();
        sparkSession.sql("SHOW TABLES FROM spark_integrition").show();
        sparkSession.sql("SELECT * FROM spark_integrition.student LIMIT 10").show();
    }

    @Test
    public void writeData() {
        Dataset<Row> student_50 = sparkSession.sql("SELECT * FROM spark_integrition.student WHERE age > 50");
        student_50.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable("spark_integrition.student_over_50");
    }

}

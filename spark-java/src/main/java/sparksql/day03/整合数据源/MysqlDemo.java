package sparksql.day03.整合数据源;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MysqlDemo {

    private SparkSession sparkSession = null;
    private JavaSparkContext jsc = null;

    @Before
    public void before() {
        // 初始化数据，连接metastore获取元信息
        sparkSession = SparkSession.builder()
                .appName("MysqlDemo")
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
    public void readData() {
        sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/spark_test")
                .option("dbtable", "student")
                .option("user", "root")
                .option("password", "root")
                .load()
                .show();
    }

    @Test
    public void writeData() {
        // 使用类创建数据行的Schema
        StructType studentSchema = new StructType()
                .add("name", "string")
                .add("age", "integer")
                .add("gpa", "float");
        // 读文件数据并转换dataframe
        String filePath = "src/main/resources/studenttab10k";
        Dataset<Row> studentDF = sparkSession
                .read()
                .option("delimiter", "\t")
                .schema(studentSchema)
                .csv(filePath);
        // 数据写到mysql中
        studentDF
                .write()
                .format("jdbc").mode(SaveMode.Overwrite)
                .option("url", "jdbc:mysql://127.0.0.1:3306/spark_test")
                .option("dbtable", "student")
                .option("user", "root")
                .option("password", "root")
                .save();
    }
}

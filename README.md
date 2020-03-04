# Spark-Java-Study
使用Java实现的Spark、SparkSQL、SparkStreaming、StructuredStreaming学习总结

## 以wordcount为例
使用SparkRDD实现
```
// 使用原生RDD实现wordcount
SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("WordCountDemo");
JavaSparkContext jsc = new JavaSparkContext(sparkConf);
jsc.setLogLevel("ERROR");
// 词频统计
String filePath = "src/main/resources/wordcount";
jsc.textFile(filePath)
        .flatMap(line -> Stream.of(line.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .collect()
        .forEach(System.out::println);
// 关闭资源
jsc.stop();
```

使用SparkSQL实现
```
// 使用sparksql实现wordcount
SparkSession sparkSession = SparkSession.builder()
        .appName("WordCountDemo")
        .master("local[2]")
        .getOrCreate();
JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
jsc.setLogLevel("ERROR");
// 词频统计
String path = "src/main/resources/wordcount";
sparkSession
        .read()
        .textFile(path)
        .flatMap(line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING())
        .groupBy("value")
        .count()
        .toDF("word", "cnt")
        .show();
// 关闭资源
jsc.stop();
```

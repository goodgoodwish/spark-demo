//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.ForeachWriter
import java.sql.Driver
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement

class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[org.apache.spark.sql.Row] {
      val driver = "org.postgresql.Driver"
      var connection:Connection = _
      var statement:Statement = _
      
    def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: org.apache.spark.sql.Row): Unit = {
        statement.executeUpdate("INSERT INTO emp_o1(id, last_name) " + 
                "VALUES (" + value(0) + ",'" + value(1) + "')")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close
      }
   }

object rdd01 {
  def main(args: Array[String]) {

    // val conf = new SparkConf().setAppName("Spark RDD demo").setMaster("local")
    // // org.apache.spark.SparkContext
    // val sc1 = new SparkContext(conf)

    // val data = Array(1, 2, 3, 4, 5)
    // val distData = sc1.parallelize(data)
    // val count = distData
    //   .map ( i => { i } )
    //   .reduce (_ + _)

    // println("\n\nSum of (1, 2, 3, 4, 5) is " + count)
    // printf(s"\n\n")

    // sc1.stop()

    // SparkContext
    // org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().appName("SQL_DataFrame")
      .master("local")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "10000s")  // fix WARN Executor: Issue communicating with driver in heartbeater
      .config("configuration key1 ", "configuration value")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // readKafkaBasic(spark);
    // readKafkaAgg(spark);
    // writeKafka(spark);
    // writeJDBC(spark)
    writeStreamJDBCSink(spark)

    spark.stop()
  }

  private def writeKafka(spark: SparkSession): Unit = {
    import org.apache.spark.sql.types._
    import java.sql.Timestamp

// remove testing files and checkpoint log
// rm /Users/charliezhu/work/log/input/*.csv
// rm -rf /Users/charliezhu/work/log/checkpoint/*

// To test it, copy file one by one to input/ folder.
// E.g.: $>  cp t4.csv ../input/

    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))

    val streamingDataFrame = spark.readStream
      .schema(mySchema)
      .csv("/Users/charliezhu/work/log/input/")

    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("topic", "topic1")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/Users/charliezhu/work/log/checkpoint/")
      .start()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
      .select(from_json($"value", mySchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")
      .select("year","rating","name")
      .groupBy("year")
      .agg(count("*"), max("rating"))

    df1.writeStream
      // .outputMode("complete")
      .outputMode("update")
      .format("console")
      // .option("truncate","false")
      .start()
      .awaitTermination()
  }

  private def readKafkaAgg(spark: SparkSession): Unit = {

    import spark.implicits._

    // val testdf = spark.read.option("delimiter", "|").option("header", true)
    //   .csv("/Users/charliezhu/work/archive/ip.dat")
    // testdf.show()

    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()

    // the bytes of the Kafka records represent UTF8 strings,
    // we can simply use a cast to convert the binary data into the correct type.
    val udf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .as[(String, String, Long)]
      .select("value", "timestamp")
      .withWatermark("timestamp", "10 minutes")  // Why writeKafka example do not need watermark ? 
      .groupBy("value")
      .agg(count("value"))

    val query = udf.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  private def readKafkaBasic(spark: SparkSession): Unit = {

    import spark.implicits._

    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()

    // the bytes of the Kafka records represent UTF8 strings,
    // we can simply use a cast to convert the binary data into the correct type.
    val udf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select("key", "value")

    val query = udf.writeStream
      .format("console")
      .start()

    query.awaitTermination()

  }

  private def writeJDBC(spark: SparkSession): Unit = {

//     import spark.implicits._

// val df = spark.read.json("/Users/charliezhu/work/test/iot_devices.json")
// df.show()

import java.sql.Driver
import java.sql.DriverManager
import java.sql.Connection

val driver = "org.postgresql.Driver"
val url = "jdbc:postgresql://localhost/scala_db?user=scala_user"
Class.forName(driver)
// var connection: Connection = null
val connection = DriverManager.getConnection(url)
println("connection is closed: ", connection.isClosed())

import java.util.Properties
val connectionProperties = new Properties()

// val emp_df = spark.read.jdbc(url, "emps", connectionProperties) 
// emp_df.show

val df2 = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/scala_db?user=scala_user")
      .option("dbtable", "(select id,last_name from emps) e ")
      // .option("user", "scala_user")
      // .option("password", "password")
      .load()
df2.printSchema

df2.createOrReplaceTempView("emp_v")
val dfo = spark.sql("select * from emp_v limit 5")

dfo.show

import org.apache.spark.sql._

    dfo
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/scala_db?user=scala_user")
      .option("dbtable", "emp_o1")
      // .option("user", "scala_user")
      // .option("password", "password")
      .mode(SaveMode.Append)
      // .mode(SaveMode.Overwrite)
      .save()

  }

  private def writeStreamJDBCSink(spark: SparkSession): Unit = {

import java.sql.Driver
import java.sql.DriverManager
import java.sql.Connection

val driver = "org.postgresql.Driver"
val url = "jdbc:postgresql://localhost/scala_db?user=scala_user"
Class.forName(driver)
// var connection: Connection = null
val connection = DriverManager.getConnection(url)
println("connection is closed: ", connection.isClosed())

import java.util.Properties
val connectionProperties = new Properties()

    import org.apache.spark.sql.types._
    import java.sql.Timestamp

    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))

    val streamingDataFrame = spark.readStream
      .schema(mySchema)
      .csv("/Users/charliezhu/work/log/input/")

streamingDataFrame.printSchema

val dfo = streamingDataFrame.select("id", "name")

import org.apache.spark.sql.streaming.ProcessingTime
// import scala.concurrent.duration._

// val query = dfo
//     .writeStream
//     .format("console")        
//     // .outputMode("complete") 
//     .trigger(ProcessingTime("5 seconds"))
//     .start()

val user =""
val pwd = ""

val writer = new JDBCSink(url, user, pwd)
val query =
  dfo
    .writeStream
    .foreach(writer)
    .outputMode("update")
    .trigger(ProcessingTime("2 seconds"))
    .start()
    
    query.awaitTermination()

  }

}




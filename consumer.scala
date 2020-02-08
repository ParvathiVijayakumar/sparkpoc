package sparkstreaming

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
object consumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("WordCount").getOrCreate()
       val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
   // val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("indaus")
    //create DStream to get data from kafka and send to spark to process
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record => record.value) // dstream value
    lines.foreachRDD{  rdd =>

      // Get the singleton instance of SparkSession
      val spark =
        SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //l res = rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).//
      val df = rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      //  res.saveAsTextFiles("C:\\work\\datasets\\output\\hydinfo")
      //res.print()
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab ")
      res.show()
      val host =
        "jdbc:mysql://learnmysql.cuae8tdqbpef.us-east-1.rds.amazonaws.com:3306/mysqldb"
    //  val prop = new java.util.Properties()
    //  prop.setProperty("user", "musername")
    //  prop.setProperty("password","mpassword")
      //prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
      ///res.write.mode(SaveMode.Append).jdbc(host,"kafkalogs",prop)
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate


    spark.stop()
  }
}


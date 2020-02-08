package com.bigdata.sparksql

import org.apache.spark.rdd
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestExecutorrrrrrrrrr {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("TestExecutorrrrrrrrrr").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    spark.conf.set("spark.eventLog.enabled", "true")
    spark.conf.set("spark.eventLog.dir", "file:///C:/Users/me/spark/logs")
    import spark.implicits._
    import spark.sql

    var counter = 1
    val data = Array(1,2,3,4,5)
    val rdd1 = sc.parallelize(data,3)
    val drdd = rdd1.foreach(x => (counter += x))
    //counter = 12

   // drdd.collect().foreach(println)
   // rdd1.collect.foreach(x=>println(x.mkString(",")))
    //println("cnt"+counter)
    rdd1.take(1).foreach(println)
    rdd1.collect().foreach(println)

    Thread.sleep(1000000);


    spark.stop()
  }
}


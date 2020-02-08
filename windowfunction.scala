package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object windowfunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("windowfunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\work\\dataset\\bank\\bank.csv"
    val df   = spark.read.format("csv").option("header","true").option("delimeter","true").option("inferSchema","true").load(data)



    spark.stop()
  }
}


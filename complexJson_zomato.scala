package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexJson_zomato {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexJson_zomato").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "E:\\work\\dataset\\data\\Zomato\\*.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.show()
    df.printSchema()


    spark.stop()
  }
}


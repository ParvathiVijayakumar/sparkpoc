 package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestNestedJson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("TestNestedJson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val df = spark.read.format("json").load("E:\\work\\dataset\\data\\zips.json")
    df.printSchema()
    val mdf = df.select($"_id",$"city",explode(($"loc")),$"pop",$"state").toDF("id","city","loc","pop","state")
    mdf.show()
    mdf.printSchema()


    spark.stop()
  }
}


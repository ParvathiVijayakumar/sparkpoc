package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object xmldataproc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("xmldataproc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\work\\dataset\\data\\books.xml"
    val df = spark.read.format("xml").option("rowTag","book").load(data)
    df.createOrReplaceTempView("book")
    val res = spark.sql("select * from book where price > 30")
    df.show

    spark.stop()
  }
}


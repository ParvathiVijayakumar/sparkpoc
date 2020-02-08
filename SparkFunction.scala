package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkFunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val df = spark.read.format("json").load("E:\\work\\dataset\\data\\stocks.json")
    val reg = "[^\\p{L}\\p{Nd}]+"
    val cols = df.columns.map(x=>x.replaceAll(reg,""))
    //val colss =cols.map(x=>x.replaceAll(" ",""))
    val ndf = df.toDF(cols:_*)
    ndf.createOrReplaceTempView("stocks")
    spark.sql("select * from stocks")
    df.show()
    df.printSchema()

    spark.stop()
  }
}


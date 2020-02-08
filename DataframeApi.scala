package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeApi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataframeApi").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\work\\dataset\\bank\\bank-full.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load(data)
    //val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimeter",";").load(data)
    df.createOrReplaceTempView("bank")
    val res = spark.sql("select * from bank where education = 'tertiary' and age > 60")
    res.show()
    //df.show
    df.printSchema()

    spark.stop()
  }
}


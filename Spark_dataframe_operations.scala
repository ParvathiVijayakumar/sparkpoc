package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark_dataframe_operations {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("Spark_dataframe_operations").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "E:\\work\\dataset\\bank\\bank-full.csv"
    //val df = spark.read.format("csv").option("header","true").option("delimeter",";").option("inferSchema","true").load(data)
    val df = spark.read.format("csv").option("header","true").option("delimeter","true").option("inferSchema","true").load(data)

    //val ndf = df.createOrReplaceTempView("tabbank")
    //val res = spark.sql("select avg(balance) from tabbank group by marital")
    //val res = df.select("*").where($"age">80 && $"marital".notEqual("married"))
    df.show

    Thread.sleep(1000000)
    spark.stop()
  }
}


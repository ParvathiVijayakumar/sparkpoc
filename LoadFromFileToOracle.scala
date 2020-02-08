package com.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LoadFromFileToOracle {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("LoadFromFileToOracle").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    // val input = args(0)
    // val output = args(1)


    import spark.implicits._
    import spark.sql

    val host = "jdbc:oracle:thin:@//parvathidb.c1jfblbrbkhy.eu-west-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

   // val data = "E:\\work\\dataset\\bank\\bank-full.csv"
    val data = args(0)
    val tab = args(1)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load(data)

    df.createOrReplaceTempView("bank")
    val res = spark.sql("select * from bank")
    res.write.mode(SaveMode.Append).jdbc(host,tab,prop)
    res.show()


    spark.stop()
  }
}


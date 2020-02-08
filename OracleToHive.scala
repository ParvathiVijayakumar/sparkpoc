package com.bigdata.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object OracleToHive {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("OracleToHive").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val tab = args(0)
    val url = "jdbc:mysql://mysql.conbyj3qndaj.ap-south-1.rds.amazonaws.com:3306/venutasks"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.com.mysql.cj.jdbc.Driver")
    val df = spark.read.jdbc(url,"emp",prop)
    df.write.mode(SaveMode.Append).format("hive").saveAsTable(tab)
    df.show

    spark.stop()
  }
}


package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkHiveJson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").enableHiveSupport().appName("sparkhivejson").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val input = args(0)
    val tab = args(1)
    val df = spark.read.format("json").option("inferSchema","true").load(input)
    df.write.format("hive").saveAsTable(tab)
    val res = spark.sql("select * from $tab")
    val ourl = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val ouser ="ousername"
    val opass = "opassword"
    val prop = new java.util.Properties()
    prop.setProperty("user",ouser)
    prop.setProperty("password",opass)
    prop.setProperty("Driver","Oracle.jdbc.driver.OracleDriver")
    res.write.jdbc(ourl,"test_oracle_parvathi",prop)

    spark.stop()
  }
}


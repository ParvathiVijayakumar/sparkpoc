package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkJsonDataProc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkJsonDataProc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
   // spark.conf().set("spark.debug.maxToStringFields", maxlength)
   // spark.conf.set("spark.debug.maxToStringFields", 100)

    val host = "jdbc:oracle:thin:@//bigdataoracle.cfpbfnzmymes.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val data = "E:\\work\\dataset\\data\\Zomato\\file1.json"
    val df   = spark.read.format("json").option("inferSchema","true").load(data)
    val reg = "[^\\p{L}\\p{Nd}]+"
    val cols = df.columns.map(x=>x.replaceAll(reg,""))
    val ndf = df.toDF(cols:_*)
    val ndf1 = ndf.select(explode($"restaurants")).toDF("restaurants")
    val ndf2 = ndf1.select("restaurants.restaurant.has_online_delivery","restaurants.restaurant.photos_url")
    ndf2.show()

    //val ndf = ndf1.withColumn("rest")
    //ndf.createOrReplaceTempView("Zomato_parvathi")
    //spark.sql("select * from Zomato_parvathi").write.mode("append").jdbc(host,"parvathi_zomato_data_task",prop)
    //val res = spark.sql("select * from Zomato_parvathi")
    //res.show(100)
    Thread.sleep(1000000);
    spark.stop()
  }
}


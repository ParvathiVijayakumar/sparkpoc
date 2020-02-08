package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object posexplode {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("posexplode").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext


      val data = "E:\\work\\dataset\\data\\posexplode.json"
      val df = spark.read.format("json").option("inferSchema","true").load(data)
      df.createOrReplaceTempView("tab")
      //explode
    val res = spark.sql("select name,explode(phone) as phone,explode(city) as city from tab")
     // val res = spark.sql("select name, c city, e email, p phone from tab lateral view explode(city) t as c lateral view explode(email) a as e lateral view explode(phone) aa as p ")
    // val res = spark.sql("select name,  city1, phone1, email1 from tab lateral view posexplode(city) a as c, city1 lateral view posexplode(phone) b as ph, phone1 lateral view posexplode(email) m as em, email1 where   c == ph==em")
   // val res = spark.sql("select name from tab lateral view posexplode(city) a as c")
      res.show()
      res.printSchema()
      //val pres = spark.sql("select name,  city1, phone1, email1 from tab lateral view posexplode(city) a as c, city1 lateral view posexplode(phone) b as ph, phone1 lateral view posexplode(email) m as em, email1 where   c == ph==em")
      //pres.show()

    spark.stop()
  }
}


package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ipdatardd {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ipdatardd").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val rdd = sc.textFile("E:\\work\\dataset\\apachespark.bigdataanalyst.in-Sep-2016")
    //1046 unique count
    //21451 full count
   // val ipdata = rdd.map(x=>x.split("- -")).map(x=>(x(0).trim)).distinct.count()
   // println("pssadadadadsd"+ipdata)
    val ipdata = rdd.map(x=>x.split("- -")).map(x=>(x(0).trim)).distinct
    //val nrdd = rdd.filter(x=>(!x.contains("first_name"))).map(x=>x.split(splitcoma)).filter(x=>x(10).contains("gmail"))

    ipdata.collect.foreach(println)

    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}


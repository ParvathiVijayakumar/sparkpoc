package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object us500 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("us500").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val rdd = sc.textFile("E:\\work\\dataset\\us-500\\us-500.csv")
    val splitcoma = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    //1)citywise count
    //val city = rdd.filter(x=>(!x.contains("first_name"))).map(x=>x.split(splitcoma)).map(x=>(x(4).replaceAll("\"",""),1)).reduceByKey((x,y)=>x+y)
    //city.collect.foreach(println)
    //2)remove - from phone number 1 and 2
    //val nrdd = rdd.filter(x=>(!x.contains("first_name"))).map(x=>x.split(splitcoma)).map(x=>(x(8).replaceAll("-",""),x(9).replaceAll("-","")))
    //3) Get only gmail info records
    //val nrdd = rdd.filter(x=>(!x.contains("first_name"))).map(x=>x.split(splitcoma)).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11)))
    val nrdd = rdd.filter(x=>(!x.contains("first_name"))).map(x=>x.split(splitcoma)).filter(x=>x(10).contains("gmail"))

    nrdd.collect.foreach(x=>println(x.mkString(",")))


    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}


package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexJsonData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ComplexJsonData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql


    spark.stop()
  }
}


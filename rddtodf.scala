package com.bigdata.sparksql

import java.lang

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object rddtodf {
  case class bankdet(age : String,job : String,marital : String,education : String,default1 : String,balance : String,housing : String,loan : String,contact : String,day : String,month : String,duration : String,campaign : String,pdays : String,previous : String,poutcome : String,y : String)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtodf").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
   // val input = args(0)
   // val output = args(1)
    //val brdd = sc.textFile(input)
   val brdd = sc.textFile("E:\\work\\dataset\\bank\\bank-full.csv")
   val output = "E:\\work\\dataset\\output7\\bank-full"
    val skip = brdd.first()
    val header = skip.replaceAll(";",",")
    val cln = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","")).map(x=>x.split(";")).map(x=>bankdet(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16)))
    import spark.implicits._
    import spark.sql
    //val df = cln.toDF(header.trim)
   // println(header)
   // val df = cln.toDF("age","job","marital","education","default1","balance","housing","loan","contact","day","month","duration","campaign","pdays","previous","poutcome","y")
   val df = cln.toDF()
   // df.createGlobalTempView("bank")
   // val res = spark.sql("select * from global_temp.bank")
    //df.createOrReplaceTempView("bank")
    //val res = spark.sql("select * from bank ")
   val res = df.select($"age").where($"marital" === "single" && $"age".gt("70"))
    res.show()
    res.write.format("csv").option("header","true").save(output)

   spark.stop()
  }
}


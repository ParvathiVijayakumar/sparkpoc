package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object HRDataGenerator {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("HRDataGenerator").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\work\\dataset\\hr\\10000 Records.csv"
    val reg = "[^\\p{L}\\p{Nd}]+"
    val df = spark.read.format("csv").option("header","true").option("delimiter",",").option("inferSchema","true").load(data)
    //Task1 starts
    val cols = df.columns.map(x=>x.replaceAll(reg,""))//task 1
    val ndf = df.toDF(cols:_*)
    //ndf.write.format("csv").option("header","true").option("delimiter","\t").save("E:\\work\\dataset\\hr\\output\\New10000Records.csv")
    //Task1 ends

    //Task2 start
     //val ndf1= ndf.withColumn("Domain",regexp_extract($"email","(?<=@)[^.]+(?=\\.)", 0))
     //ndf1.createOrReplaceTempView("hrtable")
    //val hrdf = spark.sql("select domain,count(domain) from  hrtable group by domain ")
   // hrdf.show()
    //Task2 ends

    //Task 3 starts
    //ndf1.printSchema()
   // val  hrdfage = spark.sql("select FirstName from hrtable where AgeinYrs = (select max(AgeinYrs) from hrtable) ")
    //hrdfage.show()
    //Task3 ends

    //Task 4 starts
    val ndf4 = ndf.withColumn("newdoj",to_date($"DateofJoining", "MM/dd/yyyy"))
    ndf4.createOrReplaceTempView("hrtable")
     //val  hrdfdoj = spark.sql("select empid,DateofJoining,rank from (select empid,DateofJoining," +
      //"rank() OVER (ORDER BY newdoj asc) rank  from hrtable) where rank = 1")
    //hrdfdoj.show()
    //DateofJoining
    //Task 4 ends

    //Task5 there is no student details in the file. Hence couldnot try this task

    //task 6 starts
    //val  hrdfsal = spark.sql("select empid,salary,rank from (select empid,salary," +
      //"rank() OVER (ORDER BY salary desc) rank  from hrtable) where rank < 11")
    //hrdfsal.show()
    //task 6 ends

    //task 7 starts
    //val  hrdfparents = spark.sql("select empid,firstname,mothersname,cnt from (select empid,firstname,mothersname," +
      "count() OVER (partition by mothersname ORDER BY empid desc) cnt  from hrtable)"//)
   // hrdfparents.show()
   val group = Window.partitionBy( "mothersname")
    ndf.withColumn("mother_cnt", count("mothersname").over(group)).
      orderBy("empid").select($"empid",$"mothersname").where($"mother_cnt">1).
      show
    //task 7 ends


    spark.stop()
  }
}


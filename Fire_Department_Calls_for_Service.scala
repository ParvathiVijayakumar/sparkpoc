package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
object Fire_Department_Calls_for_Service {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Fire_Department_Calls_for_Service").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Fire_Department_Calls_for_Service").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Fire_Department_Calls_for_Service").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val input = args(0)
    val input ="E:\\work\\dataset\\data\\Fire_Department_Calls_for_Service.csv"
    //

    //https://catalog.data.gov/dataset/fire-department-calls-for-service


    /*
        val filteredFireServiceCallRDD =sc.textFile(input)
        // NUMBER OF RECORDS IN THE FILE
        val totalRecords = filteredFireServiceCallRDD.count()
        println(s"Number of records in the data file: $totalRecords")
        // Q1: HOW MANY TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?
        println(s"Q1: HOW MANY TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?")
        val distinctTypesOfCallsRDD = filteredFireServiceCallRDD.map(x => x(3))
        distinctTypesOfCallsRDD.distinct().collect().foreach(println)
        // Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?
        println(s"Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?")
        val distinctTypesOfCallsSortedRDD = distinctTypesOfCallsRDD.map(x => (x, 1)).reduceByKey((x, y) => (x + y)).map(x => (x._2, x._1)).sortByKey(false)
        distinctTypesOfCallsSortedRDD.collect().foreach(println)
        // Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?
        println(s"Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?")
        val fireServiceCallYearsRDD = filteredFireServiceCallRDD.map(convertToYear).map(x => (x, 1)).reduceByKey((x, y) => (x + y)).map(x => (x._2, x._1)).sortByKey(false)
        fireServiceCallYearsRDD.take(20).foreach(println)
        // Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?
        println(s"Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?")
        val last7DaysServiceCallRDD = filteredFireServiceCallRDD.map(convertToDate).map(x => (x, 1)).reduceByKey((x, y) => (x + y)).sortByKey(false)
        last7DaysServiceCallRDD.take(7).foreach(println)
        // Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?
        println(s"Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?")
        val neighborhoodDistrictCallsRDD = filteredFireServiceCallRDD.filter(row => (convertToYear(row) == "2016")).map(x => x(31)).map(x => (x, 1)).reduceByKey((x, y) => (x + y)).map(x => (x._2, x._1)).sortByKey(false)
        neighborhoodDistrictCallsRDD.collect().foreach(println)
    */

    val fireServiceCallDF= spark.read.format("csv").option("header","true").option("inferSchema","true").load(input)
    // NUMBER OF RECORDS IN THE FILE
    val totalRecords = fireServiceCallDF.count()
    println(s"Number of records in the data file: $totalRecords")
    // Q1: HOW MANY TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?
    println(s"Q1: HOW MANY TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?")
    val distinctTypesOfCallsDF = fireServiceCallDF.select("`Call Type`").distinct()
    distinctTypesOfCallsDF.collect().foreach(println)
    // Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?
    println(s"Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?")
    val distinctTypesOfCallsSortedDF = fireServiceCallDF.select("`Call Type`").groupBy("CallType").count()
    //.orderBy(desc("count"))
    distinctTypesOfCallsSortedDF.collect().foreach(println)
    // Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?
    println(s"Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?")
    val fireServiceCallYearsDF = fireServiceCallDF.select("CallYear").groupBy("CallYear").count().orderBy($"count".desc)
    fireServiceCallYearsDF.show()
    // Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?
    println(s"Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?")
    val last7DaysServiceCallDF = fireServiceCallDF.select("CallDateTS").groupBy("CallDateTS").count().orderBy($"CallDateTS".desc)
    last7DaysServiceCallDF.show(7)
    // Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?
    println(s"Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?")
    val neighborhoodDistrictCallsDF = fireServiceCallDF.filter("CallYear == 2016").select("NeighborhooodsDistrict").groupBy("NeighborhooodsDistrict").count()
    //.orderBy(desc("count"))
    neighborhoodDistrictCallsDF.show()

    /////////////////////same you can do using sql ////////////////////////
    // NUMBER OF RECORDS IN THE FILE

    /*
        fireServiceCallDF.createOrReplaceTempView("fireServiceCallsView")
        val totalRecords = spark.sql("SELECT COUNT(*) from fireServiceCallsView")

        println(s"Number of records in the data file")
        totalRecords.show()
        // Q1: HOW MANY TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?
        println(s"Q1: HOW MANY DIFFERENT TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?")
        val distinctTypesOfCallsDF = spark.sql("SELECT DISTINCT CallType from fireServiceCallsView")
        distinctTypesOfCallsDF.collect().foreach(println)
        // Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?
        println(s"Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?")
        val distinctTypesOfCallsSortedDF = spark.sql("SELECT CallType, COUNT(CallType) as count from fireServiceCallsView GROUP BY CallType ORDER BY count desc")
        distinctTypesOfCallsSortedDF.collect().foreach(println)
        // Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?
        println(s"Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?")
        val fireServiceCallYearsDF = spark.sql("SELECT CallYear, COUNT(CallYear) as count from fireServiceCallsView GROUP BY CallYear ORDER BY count desc")
        fireServiceCallYearsDF.show()
        // Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?
        println(s"Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?")
        val last7DaysServiceCallDF = spark.sql("SELECT CallDateTS, COUNT(CallDateTS) as count from fireServiceCallsView GROUP BY CallDateTS ORDER BY CallDateTS desc")
        last7DaysServiceCallDF.show(7)
        // Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?
        println(s"Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?")
        val neighborhoodDistrictCallsDF = spark.sql("SELECT NeighborhooodsDistrict, COUNT(NeighborhooodsDistrict) as count from " +
          "fireServiceCallsView WHERE CallYear == 2016 GROUP BY NeighborhooodsDistrict ORDER BY count desc")
          neighborhoodDistrictCallsDF.collect().foreach(println)
    */

    ///////////////////////////////// dataset api ///////////////////
    /*   val fireServiceCallDS=fireServiceCallDF.as[firecc]
       // NUMBER OF RECORDS IN THE FILE
       import org.apache.spark.sql.functions.{avg, count}

       val totalRecords = fireServiceCallDS.count()
       println(s"Number of records in the data file: "+totalRecords)
       // Q1: HOW MANY TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?
       println(s"Q1: HOW MANY TYPES OF CALLS WERE MADE TO THE FIRE SERVICE DEPARTMENT?")
       val distinctTypesOfCallsDS = fireServiceCallDS.select($"CallType")
       distinctTypesOfCallsDS.distinct().collect().foreach(println)
       // Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?
       println(s"Q2: HOW MANY INCIDEDNTS OF EACH CALL TYPE WHERE THERE?")
       val distinctTypesOfCallsSortedDS = fireServiceCallDS.select(fireServiceCallDS("CallType")).groupBy(fireServiceCallDS("CallType")).agg(count($"CallType").alias("count")).orderBy($"count".desc)

       distinctTypesOfCallsSortedDS.collect().foreach(println)
       // Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?
       println(s"Q3: HOW MANY YEARS OF FIRE SERVICE CALLS IS IN THE DATA FILES AND INCIDENTS PER YEAR?")
       val fireServiceCallYearsDS = fireServiceCallDS.select($"CallYear").groupBy(fireServiceCallDS("CallYear")).agg(count(fireServiceCallDS("CallYear")).alias("count")).orderBy($"count".desc)

       fireServiceCallYearsDS.show()
       // Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?
       println(s"Q4: HOW MANY SERVICE CALLS WERE LOGGED IN FOR THE PAST 7 DAYS?")
       val last7DaysServiceCallDS = fireServiceCallDS.select(fireServiceCallDS("CallDateTS")).groupBy(fireServiceCallDS("CallDateTS")).agg(count(fireServiceCallDS("CallDateTS")).alias("count")).orderBy($"CallDateTS".desc)
       last7DaysServiceCallDS.show(7)
       // Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?
       println(s"Q5: WHICH NEIGHBORHOOD IN SF GENERATED THE MOST CALLS LAST YEAR?")
       val neighborhoodDistrictCallsDS = fireServiceCallDS.filter("CallYear == 2016").select(fireServiceCallDS("NeighborhooodsDistrict")).groupBy(fireServiceCallDS("NeighborhooodsDistrict")).agg(count(fireServiceCallDS("NeighborhooodsDistrict")).alias("count")).orderBy($"count".desc)
       neighborhoodDistrictCallsDS.collect().foreach(println)
       */

    spark.stop()
  }
}
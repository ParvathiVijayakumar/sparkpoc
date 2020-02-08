package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetOracleData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("GetOracleData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    // val input = args(0)
    // val output = args(1)

    import spark.implicits._
    import spark.sql
    val host = "jdbc:oracle:thin:@//parvathidb.c1jfblbrbkhy.eu-west-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    //com.microsoft.sqlserver.jdbc.SQLServerDriver==== for MS SQL
    //oracle.jdbc.OracleDriver==for ORACLE
    //com.mysql.cj.jdbc.Driver==== for mysql
   val query = "(select emp.* from emp join" +
      "                         dept on dept.deptno = emp.deptno " +
      "where emp.sal > 1000) emptest"
    //val query = "(select emp.* from emp,dept where emp.deptno = dept.deptno and emp.sal > 1000) emptest"


    val df = spark.read.jdbc(host,query,prop)
    df.show()


    spark.stop()
  }
}


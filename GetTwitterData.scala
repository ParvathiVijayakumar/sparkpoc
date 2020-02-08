package sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object GetTwitterData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("GetTwitterData").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    //val sc = spark.sparkContext
    val APIkey = "5mUmferFUw0iNmWXIB79WbjmM"//APIKey
    val APIsecretkey = "cTSGjD5YdZCq4dNN0zEuowymt5mEypswmliC3Zotd9T9g5ahTv"// (API secret key)
    val Accesstoken = "181460431-VLG5S3QJeGq6LQsy7b3TqVVi1e92VcAKEMgf4FhX" //Access token
    val Accesstokensecret = "1qqTymABE8TDwAPyWs5B07FveqvPQtHjo7pJBeoB9zcjk" //Access token secret
    import spark.implicits._
    import spark.sql
    val searchFilter = "tensorflow,Artificial Intelligence,bigdata"

    val interval = 10
    //  import spark.sqlContext.implicits._
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)

    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))
    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString)) //create dstream

    tweetStream.foreachRDD{  rdd =>

      // Get the singleton instance of SparkSession
      val spark =
        SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      import java.util.Date

      //l res = rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).//
      val df = rdd.map(x =>( x.getText,x.getUser().getScreenName())).toDF("twit","user")
      //  res.saveAsTextFiles("C:\\work\\datasets\\output\\hydinfo")
      //res.print()
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab ")
      res.show(false)
     // val host ="jdbc:mysql://learnmysql.cuae8tdqbpef.us-east-1.rds.amazonaws.com:3306/mysqldb"
      //  val prop = new java.util.Properties()
      //  prop.setProperty("user", "musername")
      //  prop.setProperty("password","mpassword")
      //prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
      ///res.write.mode(SaveMode.Append).jdbc(host,"kafkalogs",prop)
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}


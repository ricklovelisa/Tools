package tools

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by QQ on 6/16/16.
  */
object classificationReport {

  private def getTodayTimeStamp: Long = {

    val currentTime = new Date()
    val todayFormat = new SimpleDateFormat("yyyy/MM/dd")

    val today = todayFormat.format(currentTime)

    todayFormat.parse(today).getTime
  }

  private def getCurrentTimeStamp: Long = {

    new Date().getTime
  }

  def main(args: Array[String]) {

    val todayTimeStamp = getTodayTimeStamp
    val currentTimestamp = getCurrentTimeStamp

    val sql = s"select * from news_info where news_time > $todayTimeStamp"

    val conf = new SparkConf()
      .setAppName("report")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println(todayTimeStamp)
    val prop = new Properties()
    prop.setProperty("user","news")
    prop.setProperty("password","news")

    sqlContext.read
      .jdbc("jdbc:mysql://222.73.57.12:3306/news", "news_info", "news_time", 1L, currentTimestamp, 4, prop)
      .registerTempTable("tempTable")

    val data = sqlContext.sql(s"select * from tempTable where news_time > $todayTimeStamp and length(industry) != 0")

    val temp = data.filter(data("industry").contains("电器"))
    println(temp.count())
  }

}

package tools

import java.text.SimpleDateFormat
import java.util.Date
import collection.JavaConversions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QQ on 6/20/16.
  */
object eventByHotWords {

  private def getDate(formatTyp: String): String = {

    val currentTime = new Date()
    val todayFormat = new SimpleDateFormat(formatTyp)

    todayFormat.format(currentTime)
  }

  private def grepKeyByValue(word: String, wordsList: Map[String, Array[String]]): Array[String] = {

    wordsList.filter(_._2.contains(word)).keys.toArray

  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("eventCluster")
      .setMaster("local")
          .set("spark.driver.host","192.168.2.90")

    val sc = new SparkContext(conf)
    val config = new JsonConfig
    config.initConfig(args(0))

    val time = getDate("yyyy-MM-dd-HH")
    val redis = RedisUtil.getRedis(config)
    val key = s"hotword:$time"
    val data = redis.hgetAll(key).toMap.map(line => {(line._1, line._2.split("\\*").filter(_.length > 0))})

    val wordsList = data.values.toArray.flatMap(x => x).distinct
    val timeStamp = new Date().getTime

    val mySQLconn = MySQLUtil.getConnect("com.mysql.jdbc.Driver", config.getValue("mysql", "info"))
    val insertSql = "insert into events (name, stock, industry, section, time) values (?, ?, ?, ?, ?)"

    try {
      val insertPrep = mySQLconn.prepareStatement(insertSql)
      wordsList.foreach(word => {
        val result = grepKeyByValue(word, data)
        val stock = result.filter(_.startsWith("st_")).map(_.replace("st_", "")).mkString(",")
        val indus = result.filter(_.startsWith("in_")).map(_.replace("in_", "")).mkString(",")
        val sect = result.filter(_.startsWith("se_")).map(_.replace("se_", "")).mkString(",")
        println(s"$word|$stock|$indus|$sect")
        insertPrep.setString(1, word)
        insertPrep.setString(2, stock)
        insertPrep.setString(3, indus)
        insertPrep.setString(4, sect)
        insertPrep.setLong(5, timeStamp)
        insertPrep.executeUpdate
      })
    } finally {
      mySQLconn.close()
    }


//    sc.textFile("hdfs://222.73.57.12:9000/user/louvain/outputSupport/level_1_wordsCommunity/p*")
  }
}

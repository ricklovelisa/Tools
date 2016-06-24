package tools

import java.util.{Date, Properties}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by QQ on 6/24/16.
  */
object Relation {

  def getDicts(path: String) = {

    val dicts = Source.fromFile(path).getLines().map(line => {
      val temp = line.split("\t")
      val key = temp(0)
      val value = temp(1).split(",")
      (key, value)
    }).toMap

    dicts
  }

  def getRelation() = {

  }

  def filterFuc(args: (String, String, String)) = {
    args._1.length + args._2.length + args._3.length > 0
  }

  def main(args: Array[String]) {

    val currentTimestamp = new Date().getTime
    val lastTimeStamp = currentTimestamp - 1000 * 60 * 60 * 24
    val conf = new SparkConf()
      .setAppName("report")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val prop = new Properties()
    prop.setProperty("user","news")
    prop.setProperty("password","news")

    sqlContext.read
      .jdbc("jdbc:mysql://222.73.57.12:3306/news", "news_info", "news_time", 1L, currentTimestamp, 4, prop)
      .registerTempTable("tempTable")

    val data = sqlContext.sql(s"select * from tempTable where news_time < $currentTimestamp " +
      s"and news_time > $lastTimeStamp")

    val newsRelationData = data.map(row => {

      (row.getString(6), row.getString(7), row.getString(8))
    }).filter(filterFuc).map(row => {
      Map("industry" -> row._1.split(",").toSeq,
          "section" -> row._2.split(",").toSeq,
          "stock" -> row._3.split(",").toSeq)
    }).collect()


    val mysqlUrl = "jdbc:mysql://222.73.57.12:3306/news?user=news&password=news&useUnicode=true&characterEncoding=utf8"
    val mysqlConn = MySQLUtil.getConnect("com.mysql.jdbc.Driver", mysqlUrl)

    Array("stock", "industry", "section").foreach(cate => {
      val result = newsRelationData.map(_(cate)).flatMap(x => x).filter(_.length > 0).map(key => {
        val tmp = newsRelationData.filter(_(cate).contains(key))
        val reStock = tmp
          .map(_ ("stock"))
          .flatMap(x => x)
          .map((_, 1))
          .groupBy(_._1)
          .map(line => (line._1, line._2.map(_._2).sum))
          .toArray.sortBy(_._2).map(_._1).take(2).mkString(",")

        val reIndustry = tmp
          .map(_("industry"))
          .flatMap(x => x)
          .map((_, 1))
          .groupBy(_._1)
          .map(line => (line._1, line._2.map(_._2).sum))
          .toArray.sortBy(_._2).map(_._1).take(2).mkString(",")

        val reSection = tmp
          .map(_("section"))
          .flatMap(x => x)
          .map((_, 1))
          .groupBy(_._1)
          .map(line => (line._1, line._2.map(_._2).sum))
          .toArray.sortBy(_._2).map(_._1).take(2).mkString(",")
        println(s"$reStock|$reIndustry|$reSection")
        (key, (reStock, reIndustry, reSection))
      })
    })




//      val sql = s"update ${cate}_relation set re_stock = ?, re_industry = ?, re_section = ? where $cate = ?"
//      println(sql)
//      try {
//        val insertPrep = mysqlConn.prepareStatement(sql)
//        dic.keys.foreach(row => {
//          insertPrep.setString(1, row)
//          insertPrep.setString(2, row)
//          insertPrep.setString(3, row)
//          insertPrep.setString(4, row)
//          insertPrep.executeUpdate()
//        })
//      }
//    })
    mysqlConn.close()
  }
}

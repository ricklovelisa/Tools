package tools

import java.util.{Date, Properties}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by QQ on 6/24/16.
  */
object Relation {

  /**
    * 计算相关股票，行业和板块（基于RDD）
    * @param sc SparkContext
    * @param cate 类别名称
    * @param data 数据
    * @param partition 分区数
    * @return
    */
  def getRelation(sc: SparkContext, cate: String, data: Array[Map[String, Array[String]]], partition: Int) = {

    val dataRDD = sc.parallelize(data.map(_(cate)).flatMap(x => x).filter(_.length > 0).toSeq, partition)

    dataRDD. map(key => {

      val tmp = data.filter(_(cate).contains(key))

      val result = Array("stock", "industry", "section").map(item => {
        val resultTmp = tmp
          .map(_(item))
          .flatMap(x => x)
          .filterNot(_ == key)
          .map((_, 1))
          .groupBy(_._1)
          .map(line => (line._1, line._2.map(_._2).sum))
          .toArray.sortWith(_._2 > _._2).map(_._1).take(2).mkString(",")
        (item, resultTmp)
      }).toMap

//      println(s"$key||${result("stock")}|${result("industry")}|${result("section")}")
      (key, result)
    })
  }

  /**
    * 计算相关股票，行业和板块
    * @param cate 类别名称
    * @param data 数据
    * @return
    */
  def getRelation(cate: String, data: Array[Map[String, Array[String]]]) = {

    data.map(_(cate)).flatMap(x => x).filter(_.length > 0).map(key => {

      val tmp = data.filter(_(cate).contains(key))

      val result = Array("stock", "industry", "section").map(item => {
        val resultTmp = tmp
          .map(_(item))
          .flatMap(x => x)
          .filterNot(_ == key)
          .map((_, 1))
          .groupBy(_._1)
          .map(line => (line._1, line._2.map(_._2).sum))
          .toArray.sortWith(_._2 > _._2).map(_._1).take(2).mkString(",")
        (item, resultTmp)
      }).toMap

//      println(s"$key||${result("stock")}|${result("industry")}|${result("section")}")
      (key, result)
    })
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
      .jdbc("jdbc:mysql://222.73.34.104:3306/news", "news_info", "news_time", 1L, currentTimestamp, 4, prop)
      .registerTempTable("tempTable")

    val data = sqlContext.sql(s"select * from tempTable where news_time < $currentTimestamp " +
      s"and news_time > $lastTimeStamp")

    val newsRelationData = data.map(row => {

      (row.getString(6), row.getString(7), row.getString(8))
    }).filter(filterFuc).map(row => {
      Map("industry" -> row._1.split(","),
          "section" -> row._2.split(","),
          "stock" -> row._3.split(","))
    }).collect()


    val mysqlUrl = "jdbc:mysql://222.73.34.104:3306/news?user=news&password=news&useUnicode=true&characterEncoding=utf8"
    val mysqlConn = MySQLUtil.getConnect("com.mysql.jdbc.Driver", mysqlUrl)

    Array("stock", "industry", "section").foreach(cate => {
      println(cate)
      val result = getRelation(cate, newsRelationData)

      val sql = s"update ${cate}_relation set re_stock = ?, re_industry = ?, re_section = ? where $cate = ?"
      try {
        val prep = mysqlConn.prepareStatement(sql)
        result.foreach(row => {

          prep.setString(1, row._2("stock"))
          prep.setString(2, row._2("industry"))
          prep.setString(3, row._2("section"))
          prep.setString(4, row._1)
          prep.executeUpdate()
        })
      }
    })

    mysqlConn.close()
  }
}

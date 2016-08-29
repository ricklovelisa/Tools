package tools

import tools.util.{MySQLUtils, HBaseUtil}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangxin on 2016/8/5.
  *
  * 初始事件源写入MySQL
  */
object eventsIn {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("eventsIn").setMaster("local")
    val sc = new SparkContext(conf)


    //从news_info 读取新闻信息 news_info
    val conn_news = MySQLUtils.getConnect("jdbc:mysql://61.147.114.76:3306/news?user=news&password=news&useUnicode=true&characterEncoding=utf8")
    val state_news = conn_news.createStatement()

    //读取事件文本
    val communities = sc.wholeTextFiles("D:\\555_TopicDNT\\源事件（筛选）").collect()
    val communitiesTemp = new ArrayBuffer[(Int, String, Array[(Int, String)], Long, Long)]()  //（ID，NAME，（url，id），starttime, endTime）

    communities.foreach(community =>{

      val urlTemp = new ArrayBuffer[(Int, String)]()
      var startTime =  19900909L
      var endTime = 19900909L

      val temp1 = community._1
      val temp2 = community._2
      val ID = temp1.substring(temp1.indexOf("[")+1, temp1.lastIndexOf("]"))
      val NAME = temp1.substring(temp1.lastIndexOf("]")+1, temp1.indexOf(".txt"))
      val relatedNews = temp2.split("\n")
      relatedNews.foreach(news => {
        val title = news.substring(news.indexOf("[")+1, news.lastIndexOf("]"))
        val url = news.substring(news.lastIndexOf("]")+1, news.length).trim
        var id = 0

        // 查新闻ID和time
        val dataResult = state_news.executeQuery("select id,news_time from news_info where url ='" + url+"'")
        while (dataResult.next()) {
          id = dataResult.getString("id").toInt
          val news_time = dataResult.getString("news_time").toLong
          if(news_time < startTime) startTime = news_time
          if(news_time > endTime) endTime = news_time
        }
        urlTemp.+=((id, url))



        //匹配
        //        if(!allDataURL.filter(_.equals(url)).isEmpty()){
        //
        //          // 查新闻ID和time
        //          val dataResult = state_news.executeQuery("select id,news_time from news_info where url ='" + url+"'")
        //          while (dataResult.next()) {
        //            id = dataResult.getString("id").toInt
        //            val news_time = dataResult.getString("news_time").toLong
        //            if(news_time < startTime) startTime = news_time
        //            if(news_time > endTime) endTime = news_time
        //          }
        //          urlTemp.+=((id, url))
        //        }

      })

      communitiesTemp.+=((ID.toInt, NAME, urlTemp.toArray, startTime, endTime))
    })

    //只取URL
    val communitiesURL = communitiesTemp.flatMap(line =>{
      line._3.map(_._2)
    })

    //从Hbase读取文章
    val hbaseConf = HBaseUtil.getHbaseConf("hdfs://61.147.114.72/hbase", "server0,server1,server2")
    val allDataURL = HBaseUtil.getRDD(sc, hbaseConf)
      .map(_.split("\n\t"))
      .filter(_.length == 3 )
      .filter(line => communitiesURL.contains(line(0)))
      .map(line => line(0))  //只取URL

    //匹配
    val communitiesMatch = communitiesTemp.map(line => {

      val IDURL = line._3.map(row => {
        val url = row._2
        if(!allDataURL.filter(_.equals(url)).isEmpty()){
          (row._1, url)
        }else{
          (row._1, "kong")
        }
      }).filter(_._2.equals("kong"))

      (line._1, line._2, IDURL, line._4, line._5)
    })

    val communitiesInsert = communitiesMatch.map(line => (line._1, line._2, line._3.map(_._1).mkString(","), line._4, line._5))

    //写入数据库  245
    val conn_event = MySQLUtils.getConnect("jdbc:mysql://61.147.114.85:3306/news?user=news&password=news&useUnicode=true&characterEncoding=utf8")

    val insertPrep = conn_event.prepareStatement("insert into events_test (id, related_news, " +
      "start_time, end_time, related_stock, stock_weight) values (?, ?, ?, ?, ?, ?)")

    // 插入新数据
    communitiesInsert.foreach(row => {

      insertPrep.setInt(1, row._1)
      insertPrep.setString(2, row._3)
      insertPrep.setLong(3, row._4)
      insertPrep.setLong(4, row._5)
      insertPrep.setString(5, "")
      insertPrep.setString(6, "")
      insertPrep.addBatch()
    })

    insertPrep.executeBatch()
    insertPrep.close()

    //关闭
    conn_event.close()
    conn_news.close()
  }
}


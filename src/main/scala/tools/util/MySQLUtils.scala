package tools.util

import java.sql.{Connection, DriverManager}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by liumiao on 2016/6/24.
  * 数据库操作
  */
object MySQLUtils {

  /**
    * 连接MySQL
    * @param jdbcUrl 数据库信息
    * @return 数据库连接
    * @author qiuqiu
    * @note rowNum:11
    */
  def getConnect(jdbcUrl: String): Connection = {

    var connection: Connection = null

    try {
      Class.forName("com.mysql.jdbc.Driver")   // Load the driver
      connection = DriverManager.getConnection(jdbcUrl)  // Setup the connection
    }
    catch {
      case e:Exception => e.printStackTrace()
    }

    connection
  }

  def getNewsIDandURL(mysqlURL: String, idList: Array[Int]) = {

    val conn = getConnect(mysqlURL)

    val state = conn.createStatement()

    val dataResult = state.executeQuery(s"select id, url from news_info where id in (${idList.mkString(",")})")

    val result = ArrayBuffer[(Int, String)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getInt("id"),
          dataResult.getString("url")
          )
      )

    }

    conn.close()
    result.toArray
  }

  /**
    * 获取events数据
    * @param mysqlURL mysqlURL
    * @author QQ
    * @note rowNum:19
    */
  def getEvents(mysqlURL: String) = {

    val conn = getConnect(mysqlURL)

    val state = conn.createStatement()

    val dataResult = state.executeQuery("select * from events_test where is_valid ")

    val result = ArrayBuffer[(Int, String, Long, Long, String, String, Int)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getInt("id"),
          dataResult.getString("related_news"),
          dataResult.getLong("start_time"),
          dataResult.getLong("end_time"),
          dataResult.getString("related_stock"),
          dataResult.getString("stock_weight"),
          dataResult.getInt("is_valid")
          )
      )

    }

    conn.close()
    result.toArray
  }

  /**
    * 获取给定时间段的新闻的URL
    * @param mysqlURL jdbcURL
    * @param startTime 起始时间
    * @param endTime 结束时间
    * @return 新闻url
    * @note rowNum:11
    */
  def getNewsURL(mysqlURL: String, startTime: Long, endTime: Long) = {

    val conn = getConnect(mysqlURL)

    val state = conn.createStatement()

    val dataResult = state.executeQuery(s"select url from news_info where type = 0 and news_time between $startTime and  $endTime")

    val result = ArrayBuffer[String]()

    while (dataResult.next()) {

      result.append(dataResult.getString("url"))

    }

    conn.close()
    result.toArray
  }


  /**
    * 获取给定新闻对应的时间戳
    * @param mysqlURL jdbcURL
    * @param urlList url集合
    * @return map[url, timeStamp]
    * @note rowNum:14
    */
  def getNewsTime(mysqlURL: String, urlList: Array[String]) = {

    val conn = getConnect(mysqlURL)

    val state = conn.createStatement()

    val dataResult = state.executeQuery(s"select url, news_time from news_info " +
      s"where type = 0 and url in ('${urlList.mkString("','")}')")

    println(s"select url, news_time from news_info " +
      s"where type = 0 and url in ('${urlList.mkString("','")}')")

    val result = ArrayBuffer[(String, Long)]()

    while (dataResult.next()) {

      result.append((dataResult.getString("url"),
        dataResult.getLong("news_time"))
      )

    }

    conn.close()
    result.toMap
  }

  /**
    * 插入并更新表
    * @param mysqlURL jdbcURL
    * @param updateData 更新数据
    * @param insertData 插入数据
    * @note rowNum:39
    */
  def insertAndUpdateEvents(mysqlURL: String,
                            updateData: Array[(Int, String, Long, Long, String, String)],
                            insertData: Array[(Int, String, Long, Long, String, String)]) = {

    val conn = getConnect(mysqlURL)

    val updatePrep = conn.prepareStatement("update events_test set related_news = ?, start_time = ?," +
      " end_time = ?, related_stock = ?, stock_weight = ? where id = ?")
    val insertPrep = conn.prepareStatement("insert into events_test (id, related_news, " +
      "start_time, end_time, related_stock, stock_weight) values (?, ?, ?, ?, ?, ?)")

    try {

      // 更新数据
      updateData.foreach(row => {

        updatePrep.setString(1, row._2)
        updatePrep.setLong(2, row._3)
        updatePrep.setLong(3, row._4)
        updatePrep.setString(4, row._5)
        updatePrep.setString(5, row._6)
        updatePrep.setInt(6, row._1)
        updatePrep.addBatch()

      })

      updatePrep.executeBatch()
      updatePrep.close()

      // 插入新数据
      insertData.foreach(row => {

        insertPrep.setInt(1, row._1)
        insertPrep.setString(2, row._2)
        insertPrep.setLong(3, row._3)
        insertPrep.setLong(4, row._4)
        insertPrep.setString(5, row._5)
        insertPrep.setString(6, row._6)
        insertPrep.addBatch()

      })

      insertPrep.executeBatch()
      insertPrep.close()

    } catch {

      case e: Exception => println(e)
    } finally {

      conn.close()
    }
  }

}

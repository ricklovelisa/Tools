package tools

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * Created by zx on 2016/3/22.
  */
object MySQLUtil {

  /**
    * 建立连接
    *
    * @param driver 注册driver
    * @param jdbcUrl jdbcurl
    * @return 返回从数据库中读取的数据
    * @author LiYu
    */
  def getConnect(driver:String, jdbcUrl:String): Connection = {
    var connection: Connection = null
    try {
      // 注册Driver
      Class.forName(driver)
      // 得到连接
      connection = DriverManager.getConnection(jdbcUrl)
      //    connection.close()
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    connection
  }


  /**
    * 获取mysql数据库中的数据
    *
    * @param sqlString 注册driver
    * @param connection jdbcurl
    * @return 返回从数据库中读取的数据
    * @author LiYu
    */
  def getResult(connection:Connection, sqlString:String) :ResultSet={

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sqlString)

    resultSet
  }

  /**
    * 按照ID区间取新闻,并封装到Map[title,content]
    *
    * @param conn  数据库连接
    * @param idBegin 起始ID
    * @param idEnd 终止ID
    * @return 新闻Map
    * @author zhangxin
    */
  def getNews(conn:Connection,idBegin:Int,idEnd:Int):Map[String,String] ={
    var result=Map[String,String]()
    val sqlstr="SELECT title,content FROM indus_text_with_label WHERE id>"+idBegin+"and id<="+idEnd
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(sqlstr)
    while ( resultSet.next() ) {
      val title= resultSet.getString("title").trim()
      val content= resultSet.getString("content").trim()
      result +=(title -> content)
    }
    result
  }

  def writeToMysql(data: (Array[(String, Int, String, String, String, String, String, String, String, String)],
    Array[(String, Int, String, String, String, String, String, String, String, String)]), mysqlUrl: String) : Unit = {

    val conn = getConnect("com.mysql.jdbc.Driver", mysqlUrl)
    val insertSql = "insert into vip_info (name, official_vip, introduction," +
      "cnfol_id, moer_id, snowball_id, taoguba_id, weibo_id, home_page, portrait) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    try {
      val insertPrep = conn.prepareStatement(insertSql)
      data._1.foreach(row => {
        insertPrep.setString(1, row._1)
        insertPrep.setInt(2, row._2)
        insertPrep.setString(3, row._3)
        insertPrep.setString(4, row._4)
        insertPrep.setString(5, row._5)
        insertPrep.setString(6, row._6)
        insertPrep.setString(7, row._7)
        insertPrep.setString(8, row._8)
        insertPrep.setString(9, row._9)
        insertPrep.setString(10, row._10)
        insertPrep.executeUpdate
      })
      println("insert completed")

      val updateSql = "update vip_info set official_vip = ?, introduction = ?, cnfol_id = ?," +
        "moer_id = ?, snowball_id = ?, taoguba_id = ?, weibo_di = ?, home_page = ?, portrait = ? where name = ?"
      val updatePrep = conn.prepareStatement(updateSql)
      data._2.foreach(row => {
        updatePrep.setInt(2, row._2)
        updatePrep.setString(3, row._3)
        updatePrep.setString(4, row._4)
        updatePrep.setString(5, row._5)
        updatePrep.setString(6, row._6)
        updatePrep.setString(7, row._7)
        updatePrep.setString(8, row._8)
        updatePrep.setString(9, row._9)
        updatePrep.setString(10, row._10)
        updatePrep.setString(1, row._1)
        updatePrep.executeUpdate
      })
      println("update completed")
    }
    finally {
      conn.close()
    }
  }

}

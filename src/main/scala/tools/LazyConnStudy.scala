package tools
import java.sql.{DriverManager, PreparedStatement}

import scala.xml.Elem

/**
  * Created by yangshuai on 2016/5/11.
  */
class LazyConnStudy(createMySQLConnection: () => PreparedStatement) extends Serializable {

  lazy val mysqlConn = createMySQLConnection()
}

object LazyConnStudy {

  def apply(configFile: Elem): LazyConnStudy = {

    val createWeiboPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      connection.prepareStatement("INSERT INTO article_weibo (user_id, title, retweet, reply, like_count, url, ts) VALUES (?,?,?,?,?,?,?)")
    }

    new LazyConnStudy(createWeiboPs)

  }

}

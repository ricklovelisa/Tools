package tools

import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import com.kunyandata.nlpsuit.util.TextPreprocessing.formatText

/**
  * Created by QQ on 2016/5/23.
  */
object ComputeBV {

  /**
    * 获取一个元素，为特定数据集创建的方法
    * @param prop 特定数据集，保存一个用户的相关信息，包括user_id,followers_count,official_vip,introduction,home_page,potrait
    * @param target 想要得到的平台，元素，以及不存在时的返回值
    * @return String
    */
  private def getElem(prop:Map[String, Map[String, String]], target: (String, String, String)) = {

    if (prop.keys.toArray.contains(target._1))
      prop(target._1)(target._2)
    else
      target._3
  }

  /**
    * 计算用户的rank
    * @param prop 用户的属性
    * @param sortedPlatformWithWeight 每个平台的权重
    * @param followerInfoByPlatform 每个平台的粉丝数量的分布
    * @return Double
    */
  private def getRank(prop:Map[String, Map[String, String]],
                      sortedPlatformWithWeight:Map[String, Double],
                      followerInfoByPlatform:Map[String, (Int, Int)]) = {

    val result = sortedPlatformWithWeight.map(platformAndWeight => {

      if (prop.keys.toArray.contains(platformAndWeight._1)){

        val folNum = prop(platformAndWeight._1)("followers_count").toDouble
        val rank = (1.0 * folNum - followerInfoByPlatform(platformAndWeight._1)._1)/
          (followerInfoByPlatform(platformAndWeight._1)._2 - followerInfoByPlatform(platformAndWeight._1)._1)

        rank * platformAndWeight._2
      } else
        0.0
    }).sum

    result
  }

  private def selectOneElem(prop:Map[String, Map[String, String]],
                            target: (String, String), sortedPlatform: Array[String]): String = {

    sortedPlatform.foreach(tableName => {

      if (prop.keys.toArray.contains(tableName))

        if (prop(tableName)(target._1).length > 0)
          return prop(tableName)(target._1)
    })

    target._2
  }

  private def getVipTables(conn: Connection, tables: Array[String]) = {

    val resultTable = tables
      .map(tableName => {
        val sql = "select * from " + tableName
        val tableTemp = MySQLUtil.getResult(conn, sql)
        val vipTable = new ArrayBuffer[(String, String, Map[String, String])]()

        while (tableTemp.next) {

          val sourceName = tableTemp.getString("name")
          val formatName = formatText(sourceName)
          val userID = tableTemp.getString("user_id")
          val officialVip = tableTemp.getInt("official_vip").toString
          val intro = tableTemp.getString("introduction")
          val followers = tableTemp.getInt("followers_count").toString
          val homePage = tableTemp.getString("home_page")
          val portrait = tableTemp.getString("portrait")
          vipTable.append((formatName, tableName, Map("source_name" -> sourceName, "user_id" -> userID,
            "official_vip" -> officialVip, "introduction" -> intro, "followers_count" -> followers,
            "home_page" ->homePage, "portrait" -> portrait)))

        }

        vipTable.toArray
      }).flatMap(_.toList)

    resultTable
  }

  private def getVipInfo(conn: Connection) = {

    val infoSql = "select * from vip_info"
    val tableTemp = MySQLUtil.getResult(conn, infoSql)
    val infoTable = new ArrayBuffer[String]()

    while (tableTemp.next) {
      val name = tableTemp.getString("name")
      infoTable.append(name)
    }

    infoTable.toArray
  }
  private def computeFollowersDistribution(vipTables: Array[(String, String, Map[String, String])]) = {

    val followersInfoByPlatform = vipTables.map(line => (line._2, line._3("followers_count").toInt))
      .groupBy(_._1).map(line => {
      val platform = line._1
      val max = line._2.map(_._2).max
      val min = line._2.map(_._2).min
      (platform, (min, max))
    })

    followersInfoByPlatform
  }

  private def formatVipTablesToVipInfo(sc: SparkContext,
                                       vipTables: Array[(String, String, Map[String, String])],
                                       platformWithWight: Map[String, Double],
                                       followersInfoByPlatform: Map[String, (Int, Int)]) = {

    val sortedPlatform = platformWithWight.toArray.sortWith(_._2 > _._2).map(_._1)
    val nameRDD = sc.parallelize(vipTables.toSeq, 4).groupBy(_._1).map(line => {
      val name = line._1
      val prop = line._2.toArray.map(item => (item._2, item._3)).toMap
      val cnfolID = getElem(prop, ("vip_cnfol", "user_id", "0"))
      val moerID = getElem(prop, ("vip_moer", "user_id", "0"))
      val snowballID = getElem(prop, ("vip_snowball", "user_id", "0"))
      val taogubaID = getElem(prop, ("vip_taoguba", "user_id", "0"))
      val weiboID = getElem(prop, ("vip_weibo", "user_id", "0"))
      val intro = selectOneElem(prop, ("introduction", "这家伙很懒，什么都没写"), sortedPlatform)
      val homePage = selectOneElem(prop, ("home_page", "对不起，我找不到家了"), sortedPlatform)
      val portrait = selectOneElem(prop, ("portrait", "null"), sortedPlatform)
      val rank = getRank(prop, platformWithWight, followersInfoByPlatform)
      val ifvip = sortedPlatform
        .map(tableName => {

          if (prop.keys.toArray.contains(tableName)) {
            if (prop(tableName)("official_vip") != "0")
              1
            else
              0
          } else
            0

        }).sum
      (name, rank, ifvip, intro, cnfolID, moerID, snowballID, taogubaID, weiboID, homePage, portrait)
    })
    nameRDD
  }

  def run(sc: SparkContext, mysqlUrl: String) = {

    val conn = MySQLUtil.getConnect("com.mysql.jdbc.Driver", mysqlUrl)

    val vipTables = getVipTables(conn, Array("vip_cnfol",
      "vip_moer", "vip_snowball", "vip_taoguba", "vip_weibo"))

    val vipInfo = getVipInfo(conn)

    // 关闭连接
    conn.close()

    // 计算每个平台的粉丝数分布信息
    val followersDistributrionByPlatform = computeFollowersDistribution(vipTables)

    val platformWithWight = Map("vip_snowball" -> 0.5, "vip_moer" -> 0.25,
      "vip_cnfol" -> 0.12, "vip_weibo" -> 0.08,  "vip_taoguba" -> 0.05)

    val nameRDD = formatVipTablesToVipInfo(sc, vipTables, platformWithWight, followersDistributrionByPlatform)
    val rankDistribution = nameRDD.map(_._2).collect.sorted
    val threshold = rankDistribution.take(Math.round(rankDistribution.length * 0.2).toInt).max

    // Compute ifVip
    val result = nameRDD.map(line => {

      if ((line._3 > 0) & (line._2 >= threshold))
        (line._1, 3, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11)
      else if ((line._3 > 0) & (line._2 < threshold))
        (line._1, 2, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11)
      else if ((line._3 == 0) & (line._2 >= threshold))
        (line._1, 1, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11)
      else
        (line._1, 0, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11)

    }).collect

    val insertData = result.filterNot(line => vipInfo.contains(line._1))
    println(insertData.length)
    val updateData = result.filter(line => vipInfo.contains(line._1))
    (insertData, updateData)
  }
}

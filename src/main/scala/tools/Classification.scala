package tools

import com.kunyandata.nlpsuit.classification.Regular
import com.kunyandata.nlpsuit.util.{TextPreprocessing, KunyanConf}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by QQ on 2016/5/26.
  */
object Classification {

  def main(args: Array[String]) {

//    val conf = new SparkConf()
//      .setAppName("Classification")
//      .setMaster("local")
//    val sc = new SparkContext(conf)
    val config = new JsonConfig
   config.initConfig(args(0))
    val mysqlUrl = config.getValue("mysql", "test")
    val conn = MySQLUtil.getConnect("com.mysql.jdbc.Driver", mysqlUrl)
    val kunyanConf = new KunyanConf
    val stockDict = Source.fromFile(config.getValue("dicts","stockWordsPath")).getLines().toArray.map(line => {
      val Array(stockCode, words) = line.split("\t")
      (stockCode, words.split(","))
    }).toMap


    kunyanConf.set(config.getValue("kunyan", "host"), config.getValue("kunyan", "port").toInt)

    config.getValue("mysql","articleTables").split(",").foreach(tableName => {
      val contents = MySQLUtil.getResult(conn, "select id, title from " + tableName)
      val updateSql = "update " + tableName + " set stock = ? where id = ?"
      val updatePrep = conn.prepareStatement(updateSql)
      while (contents.next) {
        val id = contents.getInt("id")
        val title = contents.getString("title")
        if (title != null) {
          if (title.length != 0) {
            println(s"### $tableName ----- $id ----- $title ---- ${title.length} ###")
            val titleSeg = TextPreprocessing.process(title, Array(""), kunyanConf)
            val category = Regular.grep(titleSeg, stockDict)
            if (category.length != 0) {
              val cateWithName = category.split(",").map(cate => {
                cate + "=" + stockDict(cate).filterNot(_ == cate)(0)
              }).mkString("&")
              try {
                updatePrep.setString(1, cateWithName)
                updatePrep.setInt(2, id)
                updatePrep.executeUpdate()
              }
            }
          }
        }
      }
    })

    conn.close()
  }
//    val table = MySQLUtil.getResult(conn, sql)
}

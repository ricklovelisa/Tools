package tools

import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 6/24/16.
  */
object MakeDicts {

  def getData(mySQLUrl: String, sql: String) = {

    val conn = MySQLUtil.getConnect("com.mysql.jdbc.Driver", mySQLUrl)
    val result = ArrayBuffer[Array[String]]()
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(sql)
    val metaData = resultSet.getMetaData
    val size = metaData.getColumnCount
    while (resultSet.next()) {
      val tempRow = ArrayBuffer[String]()
      for (i <- 1 to size) {
        val temp = resultSet.getString(i)
        tempRow.append(temp)
      }
      result.append(tempRow.toArray)
    }
    conn.close()

    result.toArray
  }

  def writeData(path: String, data: Array[(String, String)]) = {

    val writer = new PrintWriter(new File(path))
    data.foreach(line => {
      writer.write(s"${line._1}\t${line._2}\n")
      writer.flush()
    })
    writer.close()
  }

  def main(args: Array[String]) {

//    val conf = new SparkConf()
//      .setAppName("Relation")
//      .setMaster("local")
//      .set("spark.driver.host", "192.168.2.90")
//
//    val sc = new SparkContext(conf)

    val mysqlUrlNews = "jdbc:mysql://61.147.114.76:3306/news?user=news&password=news&useUnicode=true&characterEncoding=utf8"
    val mysqlUrlStock = "jdbc:mysql://61.147.114.73:3306/backtest?user=backtest&password=seCoF7xeXzGUd8LvsM&useUnicode=true&characterEncoding=utf8"
    val stockCodeSql = "select symbol, sename from bt_stcode where (EXCHANGE = '001002' or " +
      "EXCHANGE = '001003') and SETYPE = '101' and CUR = 'CNY' and ISVALID = 1 and LISTSTATUS <> '2'"
    val stockGnSeSql = "select * from SH_SZ_BOARDMAP"
    val stockCodeData = getData(mysqlUrlStock, stockCodeSql)
//    val stockGNSEData = getData(mysqlUrlStock, stockGnSeSql)

    val stockData = stockCodeData.map(line => {
      (line(0), line.mkString(","))
    })
//    val industry = stockGNSEData.filter(_(1) == "1109").map(_(2)).distinct
//    val industryData = industry.map(indus => {
//      val stockCodes = stockGNSEData.filter(_(2) == indus).map(_(0))
//      val stockNames = stockCodes.map(code => {
//        stockCodeData.filter(_(0) == code).map(_(1))
//      }).flatMap(x => x)
//      (indus, Array(stockCodes, stockNames).flatMap(x => x).mkString(","))
//    })
//
//    val section = stockGNSEData.filter(_(1) == "1105").map(_(2)).distinct
//    val sectionData = section.map(sect => {
//      val stockCodes = stockGNSEData.filter(_(2) == sect).map(_(0))
//      val stockNames = stockCodes.map(code => {
//        stockCodeData.filter(_(0) == code).map(_(1))
//      }).flatMap(x => x)
//      (sect, Array(stockCodes, stockNames).flatMap(x => x).mkString(","))
//    })


    writeData("/home/QQ/working/util/dicts/stock_words.words", stockData)
//    writeData("/home/QQ/working/util/dicts/industry_words.words", industryData)
//    writeData("/home/QQ/working/util/dicts/section_words.words", sectionData)



  }
}

package tools

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by QQ on 2016/5/25.
  */
object Doit {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Insert BV")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val mysqlConfig = new JsonConfig
    mysqlConfig.initConfig(args(0))
    val mysqlUrl = mysqlConfig.getValue("mysql", "test")

    val data = ComputeBV.run(sc, mysqlUrl)
    MySQLUtil.writeToMysql(data, mysqlUrl)
  }
}

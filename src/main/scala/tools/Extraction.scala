package tools

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by QQ on 2016/6/6.
  */
object Extraction {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordExtraction")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")

    val sc = new SparkContext(conf)

    val config = new JsonConfig
    config.initConfig(args(0))

    // 执行
    WordExtractor.run(sc, config)

    sc.stop()
  }
}

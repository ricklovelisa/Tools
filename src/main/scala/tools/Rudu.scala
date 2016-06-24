package tools

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QQ on 6/23/16.
  */
object Rudu {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordExtraction")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/QQ/working/cosine/data/")
  }
}

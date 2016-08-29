package tools

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QQ on 7/27/16.
  */
object TDP {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Warren_Topic_Detecting_Tacking")
      .set("dfs.replication", "1")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")

    val sc = new SparkContext(conf)

    sc.textFile("hdfs://61.147.80.249:9000/telecom/shdx/origin/data/2016-07-23/1.tar.gz", 4).filter(_.contains("youchaojiang")).foreach(println)
  }
}

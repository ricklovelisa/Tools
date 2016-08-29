package tools

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QQ on 8/1/16.
  */
object HDFStest {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("HDFStest")

    val sc = new SparkContext(conf)

//    val k = sc.textFile("hdfs://61.147.80.249:9000/telecom/shdx/origin/data/2016-07-23/1.tar.gz")
//    println(k.count())

    val y = sc.textFile("hdfs://192.168.1.249:9000/")
    println(y.count())
  }
}

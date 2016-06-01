package tools

import org.apache.spark.{SparkConf, SparkContext}
import java.net.{URLDecoder, URLEncoder}

/**
  * Created by QQ on 2016/5/31.
  */
object dataCleaning {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("DataCleaning")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:/14")
//    val da = data.map(line => {
//      val temp = line.split("\t")
//      (temp(1), temp(7).split(" "))
//    }).groupByKey().map(line => {
//      val tmp = line._2.flatMap(_.toList)
//      (line._1, tmp)
//    })

    val kk = data.map(line => {
      val temp = line.split("\t")
      temp(7).split(",")
    }).flatMap(_.toList)

    kk.filter(line => line.count(_ == '%') > 1).foreach(println)
    println(URLDecoder.decode("%25u660E%25u56ED%25u68EE%25u6797%25u90FD%25u5E02".replace("25",""), "Bytes"))


    println("%25u660E%25u56ED%25u68EE%25u6797%25u90FD%25u5E02".getBytes("Unicode").toSeq)
  }
}

package tools

import java.util.Date

import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QQ on 6/15/16.
  */
object Experiment {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:/222.73.34.97:/disk_sdc/telecom/SearchEngineData/2016-06-15/")
    println(data.count)
//    val k = new Array[Int](3000)
//
//    val r = sc.parallelize(Seq(sc.parallelize(k.toSeq).zipWithUniqueId().map(_._2.toString).collect()))
//
//    println(r.count)
//    val time1 = new Date().getTime
//    val rr = RDDandMatrix.computeNumerator(sc, r, 0, 4, 4, 900).collect()
//    val time2 = new Date().getTime
//
//    val time3 = new Date().getTime
//    val rrr = RDDandMatrix.computeNumerator(sc, r, 0, 4).collect()
//    val time4 = new Date().getTime
//
//    println(time2 - time1)
//    println(time4 - time3)
//
//    sc.stop()
  }
}

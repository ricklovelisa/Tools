package tools

import java.io.{File, FileInputStream, ObjectInputStream, Serializable}

import org.apache.spark.{SparkConf, SparkContext}
import java.net.{URLDecoder, URLEncoder}
import java.util.regex.Pattern

import com.kunyandata.nlpsuit.classification.Bayes
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/5/31.
  */
object dataCleaning {

  def change8To16(code: String): String = {

    val url = code.split("%u")
    val arr = ArrayBuffer[String]()

    if (!url(0).isEmpty) {
      arr += url(0)
    }
    for (i <- 1 until url.length) {

      if (!url(i).isEmpty && url(i).length == 4) {
        arr += ("%" + url(i).substring(0, 2))
        arr += ("%" + url(i).substring(2))
      }

    }

    val processedUrl = arr.mkString("")

    processedUrl
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("DataCleaning")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val kunyanConf = new KunyanConf
    kunyanConf.set("222.73.57.17", 16003)

    val patternUTF8 = Pattern.compile("%([0-9a-fA-F]){2}")
    val patternUnicode = Pattern.compile("%u([0-9a-fA-F]){4}")
//    sc.textFile("D:/14").coalesce(4).foreach(line => {
//      val temp = line.split("\t")
////      val wordsTemp = temp(7).replaceAll("25", "")
//      if (patternUnicode.matcher())
//      while () {
//
//      }
//      println(temp(1), wordsTemp)
//      if (patternUTF8.matcher(wordsTemp).find)
//        (temp(1), URLDecoder.decode(wordsTemp, "UTF-8"))
//      else if (patternUnicode.matcher(wordsTemp).find)
//        (temp(1), URLDecoder.decode(change8To16(wordsTemp), "UTF-16"))
//      else
//      (temp(1), wordsTemp)
//
//    })


    val k = "fadfa%25u6587%25u6021%25u82B1%25u56ED%2520%25u4E09%25u5C45"
    println(k.substring(5,8))
    var i = 1
    val xx = patternUTF8.matcher(k)
    while (xx.find()) {
      println(xx.start())
      println(xx.end())
      println(xx.group())
      println()
      val result = URLDecoder.decode(xx.group(), "UTF-8")
    }

  }
}

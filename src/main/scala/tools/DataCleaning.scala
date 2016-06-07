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
object DataCleaning {

  private def change8To16(code: String): String = {

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

  def urlcodeProcess(str: String): String = {

    val findUtf8 = Pattern.compile("%([0-9a-fA-F]){2}").matcher(str).find
    val findUnicode = Pattern.compile("%u([0-9a-fA-F]){4}").matcher(str).find

    if (findUtf8 && !findUnicode)
      urlcodeProcess(URLDecoder.decode(str, "UTF-8"))
    else if (findUnicode && !findUtf8)
      urlcodeProcess(URLDecoder.decode(change8To16(str), "UTF-16"))
    else if (findUnicode && findUtf8 && (str.contains("%20") || str.contains("%25")))
      urlcodeProcess(str.replaceAll("%20", " ").replaceAll("%25", "%"))
    else if (findUnicode && findUtf8 && !(str.contains("%20") || str.contains("%25")))
      urlcodeProcess(URLDecoder.decode(change8To16(str.replaceAll("%([0-9a-fA-F]){2}", "")), "UTF-16"))
    else
      str

  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("DataCleaning")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val kunyanConf = new KunyanConf
    kunyanConf.set("222.73.57.17", 16003)


    val data = sc.textFile("D:/2").coalesce(4).map(line => {
      val temp = line.split("\t")
      val result = urlcodeProcess(temp(7))
      (temp(1), result)
    })

    data.foreach(println)
    println(data.count())




  }
}

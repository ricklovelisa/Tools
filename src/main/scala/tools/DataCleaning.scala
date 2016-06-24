package tools

import org.apache.spark.{SparkConf, SparkContext}
import java.net.{URLDecoder, URLEncoder}
import java.util.regex.Pattern
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import scala.collection.mutable.ArrayBuffer

/**
  * 清洗电信数据中的脏数据和无效数据，并将urlcode转化为中文字符
  * Created by QQ on 2016/5/31.
  */
object DataCleaning {

  /**
    * 将包含unicode编码的urlcode用utf16的方式格式化
    * @param str urlcode
    * @return 返回一个utf16的urlcode
    */
  def change8To16(str: String): String = {

    val url = str.split("%")
    val arr = ArrayBuffer[String]()

    if (!url(0).isEmpty) {
      arr += url(0)
    }
    for (i <- 1 until url.length) {

      if (!url(i).isEmpty && url(i).length == 5) {
        arr += ("%" + url(i).substring(1, 3))
        arr += ("%" + url(i).substring(3))
      } else if (!url(i).isEmpty && url(i).length == 2) {
        val tmp = URLDecoder.decode("%" + url(i), "UTF-8")
        arr += tmp
      }

    }

    arr.mkString("")
  }

  /**
    * urlcode解码方法
    * @param str urlcode字符串
    * @return 解码后的中文字符串
    */
  def urlcodeProcess(str: String): String = {

    val findUtf8 = Pattern.compile("%([0-9a-fA-F]){2}").matcher(str).find
    val findUnicode = Pattern.compile("%u([0-9a-fA-F]){4}").matcher(str).find

    if (findUtf8 && !findUnicode) {
      urlcodeProcess(URLDecoder.decode(str, "UTF-8"))
    }
    else if (findUnicode) {
      urlcodeProcess(URLDecoder.decode(change8To16(str), "UTF-16"))
    }
    else
      str

  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("DataCleaning")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val config = new JsonConfig
    config.initConfig("D:/config.json")
    val kunyanConf = new KunyanConf
    kunyanConf.set("222.73.57.17", 16003)

    val elemList = config.getValue("dataCleaning", "elemList").split("\t").map(line => {
      (line.split(",")(0), line.split(",")(1))
    }).toMap
    val data = sc.textFile("D:/2").coalesce(4).map(line => {
      val temp = line.split("\t")
//      val result = urlcodeProcess(temp(7), elemList)
//      (temp(1), result)
    })

    data.foreach(println)
    println(data.count())




  }
}

import java.net.{Socket, URLDecoder}
import java.util.regex.Pattern

import com.kunyandata.nlpsuit.util.WordSegment
import tools._

import scala.xml.Elem

/**
  * Created by QQ on 2016/6/6.
  */
object test {

  def main(args: Array[String]) {

    val elemList = Map("%20" -> " ", "%25" -> "%", "%2C" -> ",")
    val k = Array("%u9753%u76D8%20%u8D85%u503C%u5BB6%u5177%u5BB6%u7535%u9F50%u5168%u5357%u5317%u901A%u900F%u8C61%u5C71%u5C0F%u533A%2C%u5BF9%u53E3%u4E09%u4E2D%u5FC3",
      "%25u5927%25u534E%25u516C%25u56ED%25u4E16%25u5BB6%25B7%25u5EB7%25u534E%25u82D1", "%25u5C0F%25u6768%25u751F%25u714E%25u5305%252C%25u72EC%25u7ACB%25u4EA7%25u6743",
    "%25u9753%25u76D8%2520%25u8D85%25u503C%25u5BB6%25u5177%25u5BB6%25u7535%25u9F50%25u5168%25u5357%25u5317%25u901A%25u900F%25u8C61%25u5C71%25u5C0F%25u533A%252C%25u5BF9%25u53E3%25u4E09%25u4E2D%25u5FC3")

    k.map(line => {
      DataCleaning.urlcodeProcess(line)
    }).foreach(println)

  }
}

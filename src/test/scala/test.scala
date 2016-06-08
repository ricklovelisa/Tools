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


    val k = "上海上海坤验数据服务有限公司"
    println(WordExtractor.splitWordsWithWindow(k, 6).toSeq)

  }
}

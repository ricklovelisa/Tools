import java.net.Socket

import com.kunyandata.nlpsuit.util.WordSegment
import tools._

import scala.xml.Elem

/**
  * Created by QQ on 2016/6/6.
  */
object test {

  def main(args: Array[String]) {

    val k = "%25u540C%25u6DA6%25B7%25u5723%25u5854%25u8DEF%25u65AF"
    println(DataCleaning.urlcodeProcess(k))
  }
}

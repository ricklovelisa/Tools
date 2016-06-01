package tools

import com.ibm.icu.text.CharsetDetector

/**
  * Created by QQ on 2016/5/31.
  */
object odecUtil {

//  def decodeUrlCode(
  /**
    * 识别字符编码
    *
    * @param html 地址编码
    * @return 字符编码
    * @author wc
    */
  def judgeChaser(html: Array[Byte]): String = {

    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()

    encoding.getName
  }
}

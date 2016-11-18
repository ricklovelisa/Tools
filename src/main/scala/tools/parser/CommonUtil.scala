package tools.parser

import java.text.SimpleDateFormat

/**
  * Created by qiuqiu on 16-10-19.
  */
object CommonUtil {

  /**
    * 获取给定时间字符串的时间戳
    * @param dateString 指定的时间字符串
    * @return 毫秒级的时间戳
    */
  def getDateTimeStamp(dateString: String): Long = {

    new SimpleDateFormat("yyyy-M-d-H").parse(dateString).getTime
  }

  /**
    * 获取制定时间戳的时间字符串
    * @param timeStamp 指定的时间戳
    * @return yyyyMMddHH格式的时间字符串
    */
  def getDateWithTimeStamp(timeStamp: Long): String ={

    new SimpleDateFormat("yyyyMMddHH").format(timeStamp)
  }
}

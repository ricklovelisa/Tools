package tools

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/6/4.
  */
object WordExtractor {

  // 清洗数据，只保留中文字符、大小写英文字符和数字
  private def washLines(content: String): String = {
    var str =""
    for (ch <- content) {
      if ((ch >= '一' && ch <= '龥')||(ch >= 'a' && ch <= 'z')||(ch >= 'A' && ch <= 'Z')||(ch >= '0' && ch <= '9'))
        str += ch
    }
    str
  }

  // 分解句子
  def splitSents(content: String): Array[String] = {

    if (content != null && !content.isEmpty)
      content.split("[,.?;:\'\"，。？；：‘“]").filterNot(_.length == 0)
    else
      Array("")
  }

  // 根据窗口大小分词（N-gram模型）
  def splitWordsWithWindow(content: String, window: Int): Array[String] = {

    val result = ArrayBuffer[String]()

    for (i <- 0 until content.length) {

      for (j <- 0 until window) {

        if (j <= content.length && (j + i + 1) <= content.length)
          result.append(content.substring(i, j + i + 1))

      }
    }

    result.toArray
  }

  private def filterFunc(par: (String, Int), threshold: Int): Boolean = {
    par._1.length > 1 && par._1.length < threshold
  }

  // 执行
  def run(sc: SparkContext, path: String, parallelism: Int, config: JsonConfig) = {

    // 获取词的窗口
    val wordWindow = sc.broadcast(config.getValue("wordExtract", "wordWindow").toInt)

    // 分割句子
    val data = sc.textFile(path).map(splitSents(_)).flatMap(_.array).repartition(parallelism)

    // 计算文本总长度
    val totalLength = data.map(_.length).reduce(_ + _)

    //最小词频
    val minWordFreq = config.getValue("wordExtract", "minWordFreq").toInt

    // 计算词频
    val wordsCount = data
      .map(washLines)
      .flatMap(splitWordsWithWindow(_, wordWindow.value)).map((_, 1))
      .reduceByKey(_ + _).filter(_._2 > minWordFreq).cache()

    // 获取待查词表
    val dictionary = wordsCount.filter(filterFunc(_, wordWindow.value)).collect()

    // 计算凝结度
  }

}

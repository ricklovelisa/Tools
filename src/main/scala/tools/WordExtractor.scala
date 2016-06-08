package tools

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/6/4.
  */
object WordExtractor {

  /**
    * 清洗数据，只保留中文字符、大小写英文字符和数字
    * @param content 文本
    * @return 返回清洗过的字符串
    */
  private def washLines(content: String): String = {
    var str =""
    for (ch <- content) {
      if ((ch >= '一' && ch <= '龥')||(ch >= 'a' && ch <= 'z')||(ch >= 'A' && ch <= 'Z')||(ch >= '0' && ch <= '9'))
        str += ch
    }
    str
  }

  /**
    * 分解句子
    * @param content 文本
    * @return 返回一个字符串数组
    */
  def splitSents(content: String): Array[String] = {

    if (content != null && !content.isEmpty)
      content.split("[,.?;:\'\"，。？；：‘“、!！]").filterNot(_.length == 0)
    else
      Array("")
  }

  /**
    * 根据窗口大小切分词组（N-gram）
    * @param content 文本
    * @param window 词窗口大小
    * @return 返回一个词数组
    */
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

  /**
    * 计算词汇的凝结度
    * @param word 需要计算凝结度的词
    * @param textLength 文本总长度
    * @param dictionary wordcount词典
    * @return 返回一个凝结度的值
    */
  private def coagulation(word: String, textLength: Int, dictionary: Map[String, Int]): Double = {

    val totalWordLength = textLength.toDouble
    var result = totalWordLength
    if (word.length > 1) {
      for (i <- 1 until word.length) {
        val rWord = word.substring(0, i)
        val lWord = word.substring(i)
        val pWord = dictionary(word) * totalWordLength //这里计算的时候用了化简的公式
        val pRword = dictionary(rWord)
        val pLword = dictionary(lWord)
        result = math.min(result, pWord / (pRword * pLword))
      }
    } else if (word.length == 1) {
      result = dictionary(word) / totalWordLength
    }

    result
  }

  /**
    * 计算词的右字信息熵
    * @param word word
    * @param textLength 文本总长度
    * @param dictionary 字典长度
    * @return
    */
  private def infoEntropy(word: String,
                          textLength: Int,
                          dictionary: Map[String, Int]): Array[(String, (String, Double))] = {

    val rInfoEntro = ArrayBuffer[(String, Double)]()
    val lInfoEntro = ArrayBuffer[(String, Double)]()
    val wordLength = word.length

    for (i <- 1 until wordLength) {
      val leftWord = word.substring(0, i)
      val rightWord = word.substring(i)

      if (leftWord.length == 1) {
        val pLeftWord = dictionary(leftWord) / textLength.toDouble
        val entropy = 0 - pLeftWord * Math.log(pLeftWord)
        lInfoEntro.append((rightWord, entropy))
      }
      if (rightWord.length == 1) {
        val pRigthWord = dictionary(rightWord) / textLength.toDouble
        val entropy = 0 - pRigthWord * Math.log(pRigthWord)
        rInfoEntro.append((leftWord, entropy))
      }
    }

    Array((rInfoEntro(0)._1, ("rightInfoEntropy", rInfoEntro(0)._2)),
          (lInfoEntro(0)._1, ("leftInfoEntropy", rInfoEntro(0)._2)))
  }

  /**
    * 筛选出需要计算的词，长度为（2-6）
    * @param par word & count
    * @param threshold 阈值
    * @return 返回布尔值
    */
  private def filterFunc(par: (String, Int), threshold: Int): Boolean = {

    par._1.length > 1 && par._1.length <= threshold
  }

  /**
    * 执行函数
    * @param sc spark程序入口
    * @param config 配置文件
    * @return 返回值为空
    */
  def run(sc: SparkContext, config: JsonConfig) = {

    // 获取文本存放地址
    val path = config.getValue("wordExtract", "textPath")

    // 获取并行度
    val parallelism = config.getValue("wordExtract", "parallelism").toInt

    // 获取词窗口
    val wordWindow = sc.broadcast(config.getValue("wordExtract", "wordWindow").toInt)

    // 分割句子
    val data = sc.textFile(path)
      .map(splitSents(_))
      .flatMap(_.array)
      .repartition(parallelism)
      .map(washLines)

    // 计算文本总长度
    val totalLengthBr = sc.broadcast(data.map(_.length).reduce(_ + _))

    //最小词频阈值，最小凝结度阈值和最小信息熵阈值
    val minWordFreqThreshold = config.getValue("wordExtract", "minWordFreqThreshold").toInt
    val coagulationThreshold = config.getValue("wordExtract", "coagulationThreshold").toDouble
    val infoEntropyThreshold = config.getValue("wordExtract", "infoEntropyThreshold").toDouble


    // 计算词频(过滤掉词频过小的词片段)
    val wordsCount = data
      .flatMap(splitWordsWithWindow(_, wordWindow.value)).map((_, 1))
      .reduceByKey(_ + _).filter(_._2 > 1).cache()

    // ______________________ test __________________________
    val dictionary = wordsCount.collect().toMap
    val dictBr = sc.broadcast(dictionary)

    val wordsCounts = wordsCount.filter(_._1.contains("颗骰子的"))
    wordsCounts.filter(_._1.length == 5).foreach(println)
    val wordRDD = wordsCounts.filter(filterFunc(_, wordWindow.value)).cache()
    val coagulationRDD = wordRDD.map(wordPair => {
      val word = wordPair._1
      val wordCoagulation = coagulation(word, totalLengthBr.value, dictBr.value)
      (word, wordCoagulation)
    })

    //_________________________________________________________

//    // 获取广播词表
//    val dictionary = wordsCount.collect().toMap
//    val dictBr = sc.broadcast(dictionary)
//
//    // 获得所有词的RDD （词的长度在2-6之间）
//    val wordRDD = wordsCount.filter(filterFunc(_, wordWindow.value)).cache()
//
//    // 获取待计算凝结度的词RDD
//    val coagulationRDD = wordRDD.map(wordPair => {
//      val word = wordPair._1
//      val wordCoagulation = coagulation(word, totalLengthBr.value, dictBr.value)
//      (word, wordCoagulation)
//    }).filter(_._1.length != wordWindow.value)
//
//
//    // 计算左右字信息熵
//    val infoEntropyRDD = wordRDD.filter(_._1.length > 1)
//      .map(wordPair => {
//      val word = wordPair._1
//      val result = infoEntropy(word, totalLengthBr.value, dictBr.value)
//      result
//    }).flatMap(_.array).groupByKey().filter(_._1.length > 1)

  }

}

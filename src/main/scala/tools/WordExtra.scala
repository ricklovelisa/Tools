package tools

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
  * Created by QQ on 2016/6/4.
  */
object WordExtra {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("digwords.version 2.0")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")

    val sc = new SparkContext(conf)
    // 获取参数
    val originalFile = sc.textFile(args(0) , 5).flatMap( line   => splitsentence( line ) )     // 按标点分句
    val frequencyThreshold = 0
    val consolidateThreshold = 0.0
    val freedomThreshold = 0.0
    val wordLength = 6
    val wordsFilter = 0
//    val numPartiton : Int = args(7).toInt
    val sourceFile = originalFile.map( line => washLines( line))                        // 洗数据
    val textLength = sourceFile.map(line => line.length).reduce( (a, b) => a + b)       // 计算总文本长度

    // 开始分词 step1 : 词频  setp2 ； 凝结度   step3 : 自由熵
    // 抽词 ，并且过滤掉词频很小的词
    val wordsTimes = sourceFile.flatMap ( (line : String ) => splitWord(line, wordLength)).map( ( word : String ) => (word ,1)).reduceByKey(_ + _ ).filter(_._2 > wordsFilter)
    val candidateWords = wordsTimes.filter( line => filterFunc( line._1.length , wordLength))          //  生成待查词列表 2-5 step1
    val wordsTimesReverse = wordsTimes.map{ case (key ,value) => ( key.reverse , value)}              //逆序词典1-6
    val disDictionary = wordsTimes.collect() // 一个 Array[String , Int]
    Sorting.quickSort(disDictionary)(Ordering.by[(String, Int), String](_._1)) //对字典进行排序
    val broadforwardDic = sc.broadcast(disDictionary)                                                  // 广播正序字典
    val consolidateRDD = candidateWords.map( line => countDoc ( line ,textLength,broadforwardDic.value)) // 计算凝结度 step2 ( String , (Int ,Double))
    val rightFreedomRDD = candidateWords.map{ case (key ,value ) => countDof ( (key ,value) ,broadforwardDic.value)} //右自由熵
    val disDictionaryReverse = wordsTimesReverse.collect()
    Sorting.quickSort(disDictionaryReverse)(Ordering.by[(String, Int), String](_._1))
    val broadbackwardDic = sc.broadcast(disDictionaryReverse)                                       // 广播逆序字典
    val leftFreedomRDD = candidateWords.map{ case (key ,value) => countDof ( (key.reverse , value) ,broadbackwardDic.value)}.map{ case (key,value) => (key.reverse, value)}
    val freedomRDD_temp = rightFreedomRDD.join(leftFreedomRDD)
    val freedomRDD = freedomRDD_temp.map( { case( key , (value1 , value2)) => ( key , math.min( value1 , value2))})
    val resultRDD = consolidateRDD.join(freedomRDD)
    val finalRDD = resultRDD.filter{ line : (String, ((Int, Double), Double)) => caculation( line._2._1._1 , line._2._1._2 ,line._2._2 ,frequencyThreshold ,consolidateThreshold ,freedomThreshold  )}// 词频 ， 凝结度， 自由商
    finalRDD.sortByKey().foreach(println)
    println(textLength)
  }

  def countDoc(word: (String , Int), TextLen: Int ,  dictionary : Array[(String ,Int)]): (String , (Int ,Double)) = {
    var tmp = TextLen * 1.0
    val len = dictionary.length
    for (num <- 1 until word._1.length){
      val Lword = word._1.substring(0, num)
      val Rword = word._1.substring(num)
      tmp = math.min(tmp, word._2 * TextLen*1.0 / (BinarySearch(Lword, dictionary, 0, len)._2 * BinarySearch(Rword, dictionary, 0, len)._2))
    }
    (word._1 , (word._2,tmp))
  }

  def BinarySearch(word: String , dictionary : Array[(String ,Int)], from: Int = 0, to: Int) : (Int, Int) = {
    var L = from
    var R = to - 1
    var mid : Int = 0
    while (L < R) {
      mid = (L + R) / 2
      if (word > dictionary(mid)._1)
        L = mid + 1
      else
        R = mid
    }
    if (word != dictionary(L)._1)
      return (-1, -1)
    (mid ,  dictionary(L)._2)
  }

  def countDof(wordLine: (String , Int) , dictionary : Array[(String ,Int)]) : (String , Double) = {
    val word = wordLine._1 // 需要计算信息熵的词
    val Len = word.length  // 词的长度
    var total = 0          // 总共
    var dof = mutable.ArrayBuffer[Int]() // dof
    var pos = BinarySearch(word, dictionary, 0, dictionary.length)._1 //
    val dictionaryLen = dictionary.length
    while( pos < dictionaryLen && dictionary(pos)._1.startsWith(word)) {
      val tmp = dictionary(pos)._1
      if (tmp.length == Len + 1) {
        val freq = dictionary(pos)._2
        dof += freq
        total += freq
      }
      pos += 1
    }
    ( word ,dof.map((x:Int) => 1.0*x/total).map((i:Double) => -1 * math.log(i)*i).foldLeft(0.0)((x0, x)=> x0 + x))
  }


  def splitsentence(content: String): ArrayBuffer[String] = {
    val greetStrings = mutable.ArrayBuffer[String]()
    val punctuation = List(',','.',';',':','!','，','。','：','；','！','?','？','、')
    var tmp = ""
    for (ch <- content){
      if (punctuation.contains(ch)) {
        if (tmp != ""){
          greetStrings += tmp
          tmp = ""
        }
      }
      else
        tmp += ch
    }
    greetStrings
  }
  def splitWord(v: String, wordLength: Int):ArrayBuffer[String] = {
    val len = v.length
    val greetStrings =  mutable.ArrayBuffer[String]()
    for (i <- 0 until len) {
      // 单词起始点位置
      var j: Int = 1 // 新词长度
      while (i + j <= len && j <= wordLength) {
        val tmp: String = v.substring(i, i + j)
        //println(tmp)
        greetStrings += tmp
        j += 1
        
      }
    }
    greetStrings
  }
  def washLines(content: String): String = {
    var str =""
    for (ch <- content) {
      if ((ch >= '一' && ch <= '龥')||(ch >= 'a' && ch <= 'z')||(ch >= 'A' && ch <= 'Z')||(ch >= '0' && ch <= '9'))
        str += ch
    }
    str
  }
  def caculation ( f : Int , c : Double , fr : Double , frequency : Int , consolidate : Double , free : Double) : Boolean = {
    f >= frequency && c >= consolidate && fr >= free
  }
  def filterFunc( len : Int , wordLength : Int) : Boolean= {
    len >= 2 && len <= wordLength - 1
  }
}

package tools

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 8/8/16.
  */
object eventsTest {


  /**
    * 获取Hbase配置
    *
    * @param rootDir hbase 根目录
    * @param ips 集群节点
    * @return hbaseConf
    * @author liumiao
    * @note rowNum  6
    */
  def getHbaseConf(rootDir: String, ips: String): Configuration = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.rootdir", rootDir)
    hbaseConf.set("hbase.zookeeper.quorum", ips)

    hbaseConf
  }

  /**
    * 识别字符编码
    *
    * @param html 地址编码
    * @return 字符编码
    * @author wc
    * @note rowNum 6
    */
  def judgeChaser(html: Array[Byte]): String = {

    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()

    encoding.getName
  }

  /**
    * 读取内容信息
    *
    * @param sc Spark程序入口
    * @return RDD[url，标题，正文]
    * @author wc
    * @note rowNum 18
    */
  def getRDD(sc: SparkContext, rootDir: String, ips: String): RDD[String] = {

    val hbaseConf = getHbaseConf(rootDir, ips)

    val tableName = "news_detail"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])

    val news = hbaseRdd.map(row => {

      val url = row._2.getRow
      val platform = row._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("platform"))
      val title = row._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val content = row._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      val time = Bytes.toLong(row._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("time")))

      val urlFormat = judgeChaser(url)
      val platformFormat = judgeChaser(platform)
      val titleFormat = judgeChaser(title)
      val contentFormat = judgeChaser(content)
      new String(url, urlFormat) + "\n\t" +
        new String(platform, platformFormat) + "\n\t" +
        new String(title, titleFormat) + "\n\t" +
        new String(content, contentFormat) + "\n\t" + time

    }).cache()

    news
  }

  /**
    * 计算每个词对应的文章的笛卡尔积
    * @param data 数据
    * @return 笛卡尔积
    * @note rowNum:12
    */
  private def cartesian(data: Array[((Int, String), Int)]) = {

    val result = new ArrayBuffer[(((Int, String), (Int, String)), Double)]

    val newMap = data.filter(_._1._1 == 0)  // 新增文档的词
    val oldMap = data.filter(_._1._1 != 0)  //事件新闻的词

    newMap.foreach(i => {
      oldMap.foreach(j => {

        val product = 1.0 * i._2 * j._2

        result.append(((i._1, j._1), product))
      })
    })

    result.toArray
  }

  /**
    * 计算分母第一步
    * @param data 数据
    * @return 返回该单词在每个新闻中的词频的平方
    * @note rowNum:5
    */
  private def productBySelf(data: Array[((Int, String), Int)]) = {

    data.map(x => {

      (x._1, 1.0 * x._2 * x._2)
    })
  }


  def compute(totalDict: RDD[(String, Array[((Int, String), Int)])]) = {

    // 计算分子
    val numerator = totalDict.flatMap(x => cartesian(x._2)).groupByKey().map(x => (x._1, ("numerator", x._2.sum)))

    // 计算分母
    val denominatorTemp = totalDict.flatMap(x => productBySelf(x._2)).reduceByKey(_ + _)
    val denominatorPairs = denominatorTemp.cartesian(denominatorTemp).filter(x => x._1._1._1 < x._2._1._1).map(line => {

      ((line._1._1, line._2._1), ("denominator", Math.sqrt(line._1._2 * line._2._2)))
    })

    // 合并分子分母，计算相似度
    val cosineWithPairs = numerator.join(denominatorPairs).map(line => {

      val temp = Array(line._2._1, line._2._2).toMap
      val corr = temp("numerator") / temp("denominator")

      (line._1._2._1, corr)
    })

    cosineWithPairs
  }

  /**
    * 计算文档之间的相似度
    * @param totalNewsData 需要计算都文档集合
    * @return 返回计算的结果
    * @note rowNum:14
    */
  def computeWithBigText(totalNewsData: RDD[Array[((Int, String), String, Int)]], threshold: Double) = {

    // 获取数据集中的词表
    // RDD[(String, Array[(Int, String, Int), Int])]
    // (词, (事件ID， 新闻URL)， 词频)
    val totalDict = totalNewsData
      .flatMap(x => x)
      .map(x => ((x._1._1, x._2), x._3))
      .reduceByKey(_ + _)    // 连接成bigtext
      .map(x => (x._1._2, ((x._1._1, "url"), x._2)))  // （词，（（事件ID，url）,词频））
      .groupByKey()
      .map(x => (x._1, x._2.toArray))

    val cosineWithEvents = compute(totalDict).reduceByKey(_ + _)
    //    cosineWithEvents.collect().foreach(println)

    cosineWithEvents.filter(_._2 > threshold)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("event_test")
      .setMaster("local")
      .set("spark.driver.host","192.168.2.90")

    val sc = new SparkContext(conf)

    val data = sc.wholeTextFiles("/home/QQ/working/event_test/events_source/*").map(row => {

      val event_name = row._1.replaceAll("file:/home/QQ/working/event_test/events_source/", "")
        .replaceAll(".txt", "").replaceAll("\\[\\d{1,}\\] ", "")

      val related_new_url = row._2.split("\n").map(x => x.split("   ").toSeq.map(_.replaceAll(" ", "").replaceAll("[\\[\\]]", ""))).toSeq

      (event_name, related_new_url)
    }).flatMap(row => {
      row._2.map(x => (row._1, x))
    }).map(row => {

      if (row._1 == "网约车新规") {

        if (row._2.head.contains("网约车") || row._2.head.contains("网约专车") || row._2.head.contains("网约车")) {

          (row._1, row._2(1))
        } else if (row._2.head.contains("滴滴") || (row._2.head.toLowerCase().contains("uber") || row._2.head.contains("优步"))) {

          ("1", "1")
        } else if (row._2.head.contains("出租车")){

          (row._1, row._2(1))
        } else {

          ("1", "1")
        }

      } else {

        (row._1, row._2(1))
      }
    }).filter(_._1 != "1")

    val newsRDD = 1






  }
}

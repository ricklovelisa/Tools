package tools.util

import java.text.SimpleDateFormat
import java.util.Date

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by liumiao on 2016/4/28.
  * hbase操作
  */
object HBaseUtil {

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

  def getRDD(sc: SparkContext, hbaseConf: Configuration): RDD[String] = {

    val tableName = "wk_detail"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])

    val news = hbaseRdd.map(x => {

      val a = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val b = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val c = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      val aFormat = judgeChaser(a)
      val bFormat = judgeChaser(b)
      val cFormat = judgeChaser(c)
      new String(a, aFormat) + "\n\t" + new String(b, bFormat) + "\n\t" + new String(c, cFormat)

    }).cache()

    news
  }

  /**
    * 读取内容信息
    *
    * @param sc Spark程序入口
    * @return RDD[url，标题，正文]
    * @author wc
    * @note rowNum 18
    */
  def getRDD(sc: SparkContext, rootDir: String, ips: String, timeRange: Int): RDD[String] = {
    val hbaseConf = getHbaseConf(rootDir, ips)

//    val tableName = "wk_detail"
    val tableName = "news_detail"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set(TableInputFormat.SCAN, setTimeRange(timeRange))

    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])

    val news = hbaseRdd.map(x => {

      val a = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val b = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val c = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      val aFormat = judgeChaser(a)
      val bFormat = judgeChaser(b)
      val cFormat = judgeChaser(c)
      new String(a, aFormat) + "\n\t" + new String(b, bFormat) + "\n\t" + new String(c, cFormat)

    }).cache()

    news
  }

  /**
    * 设置时间范围
    *
    * @return 时间范围
    * @author yangshuai
    * @note rowNum 15
    */
  private def setTimeRange(range: Int): String = {

    val scan = new Scan()
    val date = new Date(new Date().getTime - 60L * 60 * 1000 * range)
    val format = new SimpleDateFormat("yyyy-MM-dd HH")
    val time = format.format(date)
    val time1 = format.format(new Date().getTime)
    val startTime = time + "-00-00"
    val stopTime = time1 + "-00-00"
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    val startRow: Long = sdf.parse(startTime).getTime
    val stopRow: Long = sdf.parse(stopTime).getTime

    scan.setTimeRange(startRow, stopRow)
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)

    Base64.encodeBytes(proto.toByteArray)
  }

  /**
    * 全部新闻提取 Hbase
    *
    * @param sc SparkContext
    * @return 新闻集合(url,content)
    * @author zhangxin
    * @note  9行
    */
  def getNews(sc: SparkContext, rootDir: String, ips: String): RDD[(String, String)] = {

    val hbaseConfig = HBaseUtil.getHbaseConf(rootDir, ips)
    val newsData = HBaseUtil.getRDD(sc, hbaseConfig)

    //提取(url, content)并输出
    newsData
      .map(_.split("\n\t"))
      .filter(_.length > 2)   //过滤空白新闻
      .map(file => (file(0), file(2)))
  }

}

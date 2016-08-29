package tools

import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by QQ on 7/6/16.
  */
object Chakanshujufenbu {

  def filterFunc(str: String) = {

    str.toDouble > 0.2 && str.toDouble < 0.3
  }

  def main(args: Array[String]) {

    val data = Source.fromFile("/home/QQ/working/wordExtract/output/wordExtract/output/part-00000").getLines().toArray.map(line => {
      val temp = line.replaceAll("[()]", "").split(",")

      temp
    })

    val writer = new PrintWriter(new File("/home/QQ/working/wordExtract/output/words_0.2_0.3"))
    data.filter(line => filterFunc(line(2))).sortBy(_(2).toDouble).foreach(line => {
      writer.write(line.mkString(",") + "\n")
      writer.flush()
    })

    writer.close()
  }
}

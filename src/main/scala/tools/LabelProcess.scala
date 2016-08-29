package tools

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by QQ on 2016/3/23.
  */
object LabelProcess extends App{

  val trainingLabelOld = Source.fromFile("D:/mlearning/trainingLabel.old").getLines().toArray
  val newIndusNameToOld = Source.fromFile("D:/mlearning/dzhToths").getLines().toArray
  val newIndusNameArray = newIndusNameToOld.map(line => {
    val temp = line.split("\t")
    val tmp = temp(1).split(",").toSeq

    (temp(0), tmp)
  })

  val result = trainingLabelOld.map(line => {
    val newCate = new ArrayBuffer[String]
    val tuple = line.split("\t")
    val oldLabel = tuple(1).split(",")
    newIndusNameArray.foreach(line => {
      line._2.foreach(oldName => {
        if(oldLabel.contains(oldName)){
          newCate.append(line._1)
        }
      })
    })

    (tuple(0), newCate.toSet.toArray)
  })

  val DataFile = new File("D:/mlearning/trainingLabel.new")
  val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
  result.foreach(x => {
    bufferWriter.write(x._1 + "\t" + x._2.mkString(",") + "\n")
  })
  bufferWriter.flush()
  bufferWriter.close()
}

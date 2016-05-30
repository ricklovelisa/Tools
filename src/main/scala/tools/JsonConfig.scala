package tools

import org.json.JSONObject
import scala.io.Source

/**
  * Created by QQ on 4/26/16.
  * 读取配置文件信息的类
  */
class JsonConfig {

  private var config = new JSONObject()

  def initConfig(path: String): Unit = {
    val jsObj = Source.fromFile(path).getLines().mkString("")
    config = new JSONObject(jsObj)
  }

  def getValue(key1: String, key2: String): String = {
    config.getJSONObject(key1).getString(key2)
  }

}
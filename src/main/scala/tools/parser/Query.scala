package tools.parser

/**
  * Created by QQ on 2016/8/30.
  */
object Query {

  /**
    * 查询条件解析方法
    * @param query 查询条件
    * @return
    */
  def parser (query: String) = {

    val queries = query.split("\\+")

    val resultTemp = queries.map(query => {

      parserByType(query)
    })

    resultTemp.mkString(",")
  }

  /**
    * 将查询条件划分为不同的查询类别，分别进行处理
    * @param query 查询条件
    * @return
    */
  private def parserByType(query: String) = {

    query.substring(0, 2) match {

      case "1:" => typeOne(query.replaceAll("1:", ""))
      case "2:" => typeTwo(query.replaceAll("2:", ""))
      case "3:" => typeThree(query.replaceAll("3:", ""))
      case "4:" => typeFour(query.replaceAll("4:", ""))
    }
  }

  /**
    * 基本面查询条件
    * @param query 查询文本
    * @return
    */
  private def typeOne(query: String): (Int, String) = {

    val biggerAndSmallerTemp = Rules.biggerAndSmaller(query, 1)

    if (biggerAndSmallerTemp._1 != 0) {

      return biggerAndSmallerTemp
    }

    val isOrNotTemp = Rules.isOrNot(query, 1)

    if (isOrNotTemp._1 != 0) {

      return isOrNotTemp
    }

    (-1, s"查询条件错误：$query")
  }

  /**
    * 技术面查询条件解析
    * @param query 查询文本
    * @return
    */
  private def typeTwo(query: String): (Int, String) = {

    val biggerAndSmallerTemp = Rules.biggerAndSmaller(query, 2)

    if (biggerAndSmallerTemp._1 != 0) {

      return biggerAndSmallerTemp
    }

    (-1, s"查询条件错误：$query")
  }

  /**
    * 资金面查询条件解析
    * @param query 查询文本
    * @return
    */
  private def typeThree(query: String): (Int, String) = {

    val biggerAndSmallerTemp = Rules.biggerAndSmaller(query, 3)

    if (biggerAndSmallerTemp._1 != 0) {

      return biggerAndSmallerTemp
    }

    (-1, s"查询条件错误：$query")
  }

  /**
    * 消息面查询条件解析
    * @param query 查询文本
    * @return
    */
  private def typeFour(query: String): (Int, String) = {

    val biggerAndSmallerTemp = Rules.biggerAndSmaller(query, 4)

    if (biggerAndSmallerTemp._1 != 0) {

      return biggerAndSmallerTemp
    }

    val isOrNotTemp = Rules.isOrNot(query, 4)

    if (isOrNotTemp._1 != 0) {

      return isOrNotTemp
    }

    val continuousTemp = Rules.continuous(query, 4)

    if (continuousTemp._1 != 0) {

      return continuousTemp
    }

    (-1, s"查询条件错误：$query")
  }
}

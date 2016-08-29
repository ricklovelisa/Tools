package tools.parser

/**
  * Created by QQ on 2016/8/30.
  */
object Query {

  def parser (text: String) = {

    val queries = text.split("\\+")

    val resultTemp = queries.map(query => {

      parserByType(query)
    })

    resultTemp.mkString(",")
  }

  private def parserByType(query: String) = {

    query.substring(0, 2) match {

      case "1:" => typeOne(query.replaceAll("1:", ""))
      case "2:" => typeTwo(query.replaceAll("2:", ""))
      case "3:" => typeThree(query.replaceAll("3:", ""))
      case "4:" => typeFour(query.replaceAll("4:", ""))
    }
  }

  private def typeOne(query: String): (Int, String) = {

    val biggerAndSmallerTemp = Rules.biggerAndSmaller(query, 1)
    val isOrNotTemp = Rules.isOrNot(query, 1)
    if (biggerAndSmallerTemp._1 != 0) {

      return biggerAndSmallerTemp
    } else if (isOrNotTemp._1 != 0) {

      return isOrNotTemp
    }

    (-1, s"查询条件错误：$query")
  }

  private def typeTwo(query: String): (Int, String) = {

    val biggerAndSmallerTemp = Rules.biggerAndSmaller(query, 2)

    if (biggerAndSmallerTemp._1 != 0) {

      return biggerAndSmallerTemp
    }

    (-1, s"查询条件错误：$query")
  }

  private def typeThree(query: String): (Int, String) = {

    val biggerAndSmallerTemp = Rules.biggerAndSmaller(query, 3)

    if (biggerAndSmallerTemp._1 != 0) {

      return biggerAndSmallerTemp
    }

    (-1, s"查询条件错误：$query")
  }

  private def typeFour(query: String) = {

    val result = Rules.biggerAndSmaller(query, 4)
  }
}

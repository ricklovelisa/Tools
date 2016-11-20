package tools.vectorop

/**
  * Created by QQ on 2016/11/20.
  */
object test {

  def main(args: Array[String]) {

    val a = Array(0.3, 0.4, 0.5, 0.1)
    val b = Array(0.3, 0.4, 0.6, 0.3)

    println(VectorWiseOp.devide(a, b).toSeq)
  }
}

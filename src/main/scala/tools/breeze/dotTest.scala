package tools.breeze

import breeze.linalg.{Vector => BVec}
import breeze.linalg._

/**
  * Created by qiuqiu on 16-11-18.
  */
object dotTest {

  def main(args: Array[String]): Unit = {

    val a = BVec(1.0, 2.0, 3.4)
    val b = 2

    println(a :* b)
  }
}

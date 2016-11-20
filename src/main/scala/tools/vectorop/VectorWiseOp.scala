package tools.vectorop

/**
  * Created by QQ on 2016/11/19.
  */
object VectorWiseOp {

  def multiply(vec1: Array[Double], vec2: Array[Double]): Array[Double] = {

    if (vec1.length != vec2.length) {

      throw new IllegalArgumentException("向量长度不同！")
    } else {

      vec1.indices.map(index => {

        vec1(index) * vec2(index)
      }).toArray
    }
  }

  def plus(vec1: Array[Double], vec2: Array[Double]): Array[Double] = {

    if (vec1.length != vec2.length) {

      throw new IllegalArgumentException("向量长度不同！")
    } else {

      vec1.indices.map(index => {

        vec1(index) + vec2(index)
      }).toArray
    }
  }

  def minus(vec1: Array[Double], vec2: Array[Double]): Array[Double] = {

    if (vec1.length != vec2.length) {

      throw new IllegalArgumentException("向量长度不同！")
    } else {

      vec1.indices.map(index => {

        vec1(index) - vec2(index)
      }).toArray
    }
  }

  def devide(vec1: Array[Double], vec2: Array[Double]): Array[Double] = {

    if (vec1.length != vec2.length) {

      throw new IllegalArgumentException("向量长度不同！")
    } else {

      vec1.indices.map(index => {

        vec1(index) / vec2(index)
      }).toArray
    }
  }
}

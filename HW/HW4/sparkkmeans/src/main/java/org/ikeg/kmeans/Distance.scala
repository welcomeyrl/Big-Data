package org.ikeg.kmeans

import scala.collection.Map

trait Distance  extends java.io.Serializable {
  val name: String

  def distance(ary1: Array[Double], ary2: Array[Double]): Double

  def closest(ary: Array[Double], mat: Array[Array[Double]]): Int

  def max(mat1: Array[Array[Double]], mat2: Map[Int, Array[Double]]) : Double

}

/**
 * refer to <http://en.wikipedia.org/wiki/Cosine_similarity>
 */
class CosineSimilarity extends Distance with java.io.Serializable {

  override val name = "COSINE_SIMILARITY"

  def distance(ary1: Array[Double], ary2: Array[Double]): Double = Util.dot(ary1, ary2) /
    (Math.sqrt(Util.dot(ary1, ary1)) * Math.sqrt(Util.dot(ary2, ary2)))

  /**
   * calculate the index of the closest array in [mat] to [ary]
   * for cosine similarity, the closer the distance to 1, the more similar two arrays are
   * @param ary
   * @param mat
   * @return
   */
  def closest(ary: Array[Double], mat: Array[Array[Double]]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until mat.length) {
      val tempDist = distance(ary, mat(i))
      if ((1 - tempDist) < closest) {
        closest = 1 - tempDist
        bestIndex = i
      }
    }
    return bestIndex
  }

  def max(mat1: Array[Array[Double]], mat2: Map[Int, Array[Double]]) : Double = {
    var maxDistance = 0.0d
    for (i <- 0 until mat1.length) {
      val tempDistance = distance(mat1(i), mat2(i))
      if (1 - tempDistance > maxDistance)
        maxDistance = 1 - tempDistance
    }
    maxDistance
  }

}

class EuclideanDistance extends Distance with java.io.Serializable {
  override val name = "EUCLIDEAN DISTANCE"

  def distance(ary1: Array[Double], ary2: Array[Double]): Double =
    Math.sqrt((ary1, ary2).zipped.map((a, b) => Math.pow((a-b), 2)).sum)


  /**
   * calculate the index of the closest array in [mat] to [ary]
   * for Euclidean distance, the smaller the distance, the more similar two arrays are
   * @param ary
   * @param mat
   * @return
   */
  def closest(ary: Array[Double], mat: Array[Array[Double]]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until mat.length) {
      val tempDist = distance(ary, mat(i))
      if ((tempDist) < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    return bestIndex
  }

  def max(mat1: Array[Array[Double]], mat2: Map[Int, Array[Double]]) : Double = {
    var maxDistance = 0.0d
    for (i <- 0 until mat1.length) {
      val tempDistance = distance(mat1(i), mat2(i))
      if (tempDistance > maxDistance)
        maxDistance = tempDistance
    }
    maxDistance
  }
}

/**
 * refer to <http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient#For_a_sample>
 */
class PearsonCorrelation extends Distance with java.io.Serializable {

  override val name = "PEARSON_CORRELATION"

  def distance(ary1: Array[Double], ary2: Array[Double]): Double = {
    var numerator = 0d
    var denominatorx = 0d
    var denominatory = 0d
    val xAvg = ary1.sum / ary1.length
    val yAvg: Double = ary2.sum / ary2.length
    for (cIdx <- 0 to ary1.length - 1) {
      if (ary1(cIdx) > 0d && ary2(cIdx) > 0d) {
        numerator += (ary1(cIdx) - xAvg) * (ary2(cIdx) - yAvg)
        denominatorx += (ary1(cIdx) - xAvg) * (ary1(cIdx) - xAvg)
        denominatory += (ary2(cIdx) - yAvg) * (ary2(cIdx) - yAvg)
      }
    }
    numerator / (Math.sqrt(denominatorx) * Math.sqrt(denominatory))
  }

  def closest(ary: Array[Double], mat: Array[Array[Double]]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until mat.length) {
      val tempDist = distance(ary, mat(i))
      if ((1 - Math.abs(tempDist)) < closest) {
        closest = 1 - Math.abs(tempDist)
        bestIndex = i
      }
    }
    return bestIndex
  }

  def max(mat1: Array[Array[Double]], mat2: Map[Int, Array[Double]]) : Double = {
    var maxDistance = 0.0d
    for (i <- 0 until mat1.length) {
      val tempDistance = distance(mat1(i), mat2(i))
      if (1 - Math.abs(tempDistance) > maxDistance)
        maxDistance = 1 - Math.abs(tempDistance)
    }
    maxDistance
  }

}

object Util {

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def dot(x: Array[Double], y: Array[Double]): Double = (for ((xi, yi) <- x zip y) yield xi * yi) sum

}


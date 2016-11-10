package org.ikeg.kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by june on 15/12/7.
 */
class KMeans(numClusters: Int, threshold: Double, sim: Distance) extends java.io.Serializable {


  override def finalize(): Unit = super.finalize()

  def train(data: RDD[(String, (Array[Double], Array[String]))]): RDD[(Int, Iterable[(String, (Array[Double], Array[String]))])] = {
    var clusters: RDD[(Int, Iterable[(String, (Array[Double], Array[String]))])] = null

    val numCols = data.first()._2._1.length
    // randomly take [numClusters] samples as centroids
    var centroids = data.takeSample(false, numClusters, 42).map(x => x._2._1)

    var maxDistance = 0.0
    var idxIter = 0
    do {
      idxIter += 1
      /* Assignment step */

      // calculate the closest centroids for each data point
      var closestCentroids = data.map(p => (sim.closest(p._2._1, centroids), p))
      // group data points by the cluster they belong
      clusters = closestCentroids.groupByKey()

      /* Update step */

      var newCentroids = clusters.mapValues(ps => centroidUpdate(ps.map(x => x._2._1), numCols)).collectAsMap()
      maxDistance = 0.0
      // calculate distance between old centroids and new centroids
      maxDistance = sim.max(centroids, newCentroids)


      // update centroids with new centroids
      for (newP <- newCentroids) {
        centroids(newP._1) = newP._2
      }
      println("Iteration " + idxIter + ": " + maxDistance)
    } while (maxDistance > threshold)
    println("Converged at iteration " + idxIter)
    clusters
  }

  /**
   *
   * @param p single point
   * @param centroids current centroids
   * @return index of the closest centroid for point p
   */
  def closestCentroid(p: Array[Double], centroids: Array[Array[Double]]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centroids.length) {
      val tempDist = sim.distance(p, centroids(i)) //p.squaredDist(centroids(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    return bestIndex
  }

  /**
   *
   * @param points points in a cluster
   * @param cols number of columns for each data point
   * @return new centroid in the cluster calculated by averaging points
   */
  def centroidUpdate(points: Iterable[Array[Double]], cols: Int): Array[Double] = {
    val numPoints = points.size
    var total: Array[Double] = Array.fill(cols)(0d)
    points.foreach(p => total = (total, p).zipped.map(_ + _))
    total.map(_ / numPoints)
  }
}

object KMeans {

  def main(args: Array[String]) {

    // comment out these two lines to allow spark info output to screen
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val result = "temp.txt"

    val errMsg = "Usage: KMeans kmeans [SparkMaster_addr] [numOfClusters] [converge_threshold] [itemusermat_loc] [movies_loc]\n" +
      "Usage: KMeans sim [movie_id] [SparkMaster_addr] [itemusermat_loc] [movies_loc]"
    val invalidMovieId = "movie_id is invalid"

    if (args.length > 1) {
      if (args(0).equals("kmeans")) {
        if (args.length < 6) {
          System.err.println(errMsg)
        } else {
          /* running kmeans algorithm */

          val sc = new SparkContext(args(1), args(0))
          val K = args(2).toInt
          val convergeThresh = args(3).toDouble
          val data = sc.textFile(args(4)).map(
            t => (t.split(" ")(0), t.split(" ").drop(1).map(_.toDouble))).join(sc.textFile(args(5)).map(t => (t.split("::")(0), t.split("::").drop(0)))).cache()

          // distance function used by KMeans; possible
          val simFunc = new EuclideanDistance // CosineSimilarity

          println("Distance function: " + simFunc.name)

          val count = data.count()
          println("Number of records " + count)

          val model = new KMeans(K, convergeThresh, simFunc)
          val clusters = model.train(data)
          clusters.foreach(c => {
            println("Cluster " + c._1 + ": ")
            c._2.take(5).foreach(m => println(m._2._2.mkString("\t")))
          })

          sc.stop()
        }
      } else if (args(0).equals("sim")) {

        /* print top [numSim] similar movies */
        val numSim = 5

        if (args.length < 5)
          System.err.println(errMsg)
        else {

          val sc = new SparkContext(args(2), args(0))
          val data = sc.textFile(args(3)).map(
            t => (t.split(" ")(0), t.split(" ").drop(1).map(_.toDouble))).join(sc.textFile(args(4)).map(t => (t.split("::")(0), t.split("::").drop(0)))).cache()
          val target = data.lookup(args(1))
          if (target.isEmpty)
            System.err.println(invalidMovieId)
          else {

            println("Top " + numSim + " similar movies to movie " + args(1) + " are:")

            val pearson = new PearsonCorrelation

            val results = data.map(d => (pearson.distance(d._2._1, target(0)._1), d)).sortBy(d => d._1).take(numSim + 1).drop(1)

            results.foreach(r => println(r._2._2._2.mkString("\t")))


          }
        }

      }
    }
    else
      println(errMsg)

    System.exit(0)
  }

}

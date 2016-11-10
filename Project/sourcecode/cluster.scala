import org.apache.spark.util.Vector
import scala.util.Random
import scala.io.Source
import scala.collection.mutable.ListBuffer

def closestPoint(p: Vector, centers: Array[Vector]): Int = {
 var index = 0
 var bestIndex = 0
 var closest = Double.PositiveInfinity
 for (i <- 0 until centers.length) {
   val tempDist = p.squaredDist(centers(i))
   if (tempDist < closest) {
	 closest = tempDist
	 bestIndex = i
   }
 }
 return bestIndex
}

def average(ps: Seq[Vector]) : Vector = {
 val numVectors = ps.size

 // the first element in the Seq
 var out = new Vector(ps(0).elements)

 // sum all the vectors, and divide it by the number of vectors present in the Seq
 for (i <- 1 until numVectors) {   
   out += ps(i)
 }

 out / numVectors
}

 val K = 100
 var i = 0
 val data = sc.textFile("hdfs://cshadoop1.utdallas.edu/xxz126730/project/twiter_5000_10000.dat").map(t => (t.split(" ",2)(0), Vector(t.split(" ",2)(1).split(' ').map(_.toDouble)) )).cache()
 var centroids = data.takeSample(false, K, 13).map(x => x._2)

 do {
   var closest = data.map(p => (closestPoint(p._2, centroids), p._2))

   //var assignment = data.map(p => (closestPoint(p._2, centroids), p._1))
   var pointsGroup = closest.groupByKey()

   //var mypointsGroup = assignment.groupByKey()
   var newCentroids = pointsGroup.mapValues(ps => average(ps.toSeq)).collectAsMap()

   for (newP <- newCentroids) {
	 centroids(newP._1) = newP._2
   }
   i =i+1
 } while (i < 11)


 var i=1;
  var results = new ListBuffer[String]()
 for((centroid, centroidI) <- centroids.zipWithIndex) {
	val ids =data.filter(p => (closestPoint(p._2, centroids) == centroidI)).map(p => p._1).collect

	//val itemCount = ratings.map(x=>x._2).distinct.count 	
	println("Cluster "+i+":")
	var t_string="";	
	ids.foreach(i => (t_string=t_string+i+" "));
	results += t_string;
	println();
	i =i+1
 }
val resultsList = results.toList
resultsList.foreach(println)



   
//spark-shell -i als.scala to run this code
//SPARK_SUBMIT_OPTS="-XX:MaxPermSize=4g" spark-shell -i als.scala

import breeze.linalg._
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ArrayBuffer

//Implementation of sec 14.3 Distributed Alternating least squares from stanford Distributed Algorithms and Optimization tutorial. 

//loads ratings from file
//(user_id, movie_id, rating)
val ratings = sc.textFile("hdfs://cshadoop1.utdallas.edu/hw4fall/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2))) 
//val ratings = sc.textFile("C:/Users/Xinwen/Desktop/hw4_dataset/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2)))

// counts unique movies
val itemCount = ratings.map(x=>x._2).distinct.count 

// counts unique user
val userCount = ratings.map(x=>x._1).distinct.count 

// get distinct movies
val items = ratings.map(x=>x._2).distinct   //(movie_id)

// get distinct user
val users = ratings.map(x=>x._1).distinct   //(user_id)

// latent factor
val k= 5  

//create item latent vectors
val itemMatrix = items.map(x=> (x,DenseVector.zeros[Double](k)))   
//Initialize the values to 0.5
// generated a latent vector for each item using movie id as key Array((movie_id,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myitemMatrix = itemMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist    //create 10 partitions, and persist in memory

//create user latent vectors
val userMatrix = users.map(x=> (x,DenseVector.zeros[Double](k)))
//Initialize the values to 0.5
// generate latent vector for each user using user id as key Array((userid,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myuserMatrix = userMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist    //create 10 partitions, and persist in memory

// group rating by items. Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (itemid,(userid,rating)) e.g  (1,(2,3))
// (itemid,(userid,rating))
val ratingByItem = sc.broadcast(ratings.map(x => (x._2,(x._1,x._3)))) 

// group rating by user.  Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (userid,(item,rating)) e.g  (1,(3,5)) 
// (userid,(item,rating))
val ratingByUser = sc.broadcast(ratings.map(x => (x._1,(x._2,x._3)))) 

var i =0
for( i <- 1 to 10){
//=============This code will update the myuserMatrix which contains the latent vectors for each user. 
	
	// joining the movie latent vector with movie ratings using movieid as key. Step 1 from Sec 14.3 which results in
	//ratItemVec: is an collect of elements of type  org.apache.spark.rdd.RDD[(String, (breeze.linalg.DenseVector[Double], (String, String)))] which means [(movieid, (item latent vector, (user_id, rating)))] e.g  Array((2,(DenseVector(0.5, 0.5, 0.5, 0.5, 0.5),(1,4)))) 
	//[(movieid, (item latent vector, (user_id, rating)))]
	val ratItemVec = myitemMatrix.join(ratingByItem.value)
	
	// regularization factor which is lambda.
	val regfactor = 1.0  
	val regMatrix = DenseMatrix.zeros[Double](k,k)  //generate an diagonal matrix with dimension k by k
	//filling in the diagonal values for the reqularization matrix.
	regMatrix(0,::) := DenseVector(regfactor,0,0,0,0).t     // transpose to match row shape
	regMatrix(1,::) := DenseVector(0,regfactor,0,0,0).t 
	regMatrix(2,::) := DenseVector(0,0,regfactor,0,0).t 
	regMatrix(3,::) := DenseVector(0,0,0,regfactor,0).t 
	regMatrix(4,::) := DenseVector(0,0,0,0,regfactor).t
   
	//cal sum(yiyit+regMatrix)  //Implementation of step 2 and step 3 and 4 
	// org.apache.spark.rdd.RDD[(String, breeze.linalg.DenseMatrix[Double])]  
	//(userid,densematrix)
	//val userbyItemMat = ratItemVec.map(x => (x._2._2._1,x._2._1*x._2._1.t + regMatrix)).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2)))
 
 	val userbyItemMat = ratItemVec.map(x => (x._2._2._1, x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix)))

	// cal sum(rui * yi) where yi is item vectors and rui is the rating. Implementation of step 5
	//org.apache.spark.rdd.RDD[(String, breeze.linalg.DenseVector[Double])] 
	//(userid,Densevector)
	val sumruiyi = ratItemVec.map(x => (x._2._2._1, x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 

 	// This join will be used in calculating sum yi yit * sum (rui *yi) for each user.
	//(userid, (part_A+regMatrix, part_B))
	val joinres = userbyItemMat.join(sumruiyi) 

	// calculates sum(yi*yit) * sum(rui *yi) this gives update of user latent vectors. Combining the results to calculate EQUATION (4)
	myuserMatrix = joinres.map(x=> (x._1, x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 
//===========================================End of update for myuserMatrix latent vector==========================================================

//===========================================Homework 4. Implement code to calculate equation 3.===================================================
//=================You will be required to write code to update myitemMatrix which is the matrix that contains the latent vector for the items
//Please Fill in your code here.

	// Step 1: Join Ratings with X factors using key u (users)
	// org.apache.spark.rdd.RDD[(String, (breeze.linalg.DenseVector[Double], (String, String)))]
	// [(user_id, (user latent vector, (movie_id, rating)))]
	val ratUserVec = myuserMatrix.join(ratingByUser.value)     

	// Step 2/3/4: compute sum(xuxut)+regMatrix where xu is user vectors, and then take the inverse 
	// org.apache.spark.rdd.RDD[(String, breeze.linalg.DenseMatrix[Double])]  
	//(movie_id,densematrix)
	//val moviebyUserMat = ratUserVec.map(x => (x._2._2._1,x._2._1*x._2._1.t + regMatrix)).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2)))

	val moviebyUserMat = ratUserVec.map(x => (x._2._2._1, x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix)))

	// Step 5: compute sum(rui * xu) where rui is rating and xu is user vectors
	//org.apache.spark.rdd.RDD[(String, breeze.linalg.DenseVector[Double])] 
	//(movie_id,Densevector)
	val sumruixu = ratUserVec.map(x => (x._2._2._1, x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 

 	// Combine the above two results to calculate sum(xuxut+regMatrix) * sum(rui * xu) for each movie, which will update movie latent vectors.
	val joinres2 = moviebyUserMat.join(sumruixu)       //(movie_id, (part_A+regMatrix, part_B))
	myitemMatrix = joinres2.map(x=> (x._1, x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 

//==========================================End of update myitemMatrix latent factor=================================================================
}






// query for user_id
val userID = readLine("Please input the user_id:")

val targetUser = myuserMatrix.filter(x => x._1==userID).toArray

var results1 = ArrayBuffer[(String, breeze.linalg.DenseVector[Double])]()
results1 += targetUser(0)
results1.foreach(println)



// query for movie_id
val movieID = readLine("Please input the movie_id:")

val targetMovie = myitemMatrix.filter(x => x._1==movieID).toArray

var results2 = ArrayBuffer[(String, breeze.linalg.DenseVector[Double])]()
results2 += targetMovie(0)
results2.foreach(println)






//======================================================Implement code to recalculate the ratings a user will give an item.====================

//Hint: This requires multiplying the latent vector of the user with the latent vector of the  item. Please take the input from the command line. and
// Provide the predicted rating for user 1 and item 914, user 1757 and item 1777, user 1759 and item 231.

//Your prediction code here

val input_user = readLine("Please input the user_id:")

val input_movie = readLine("Please input the movie_id:")

val uid = myuserMatrix.filter(x => x._1==input_user).map(x => (1, x._2.t))     // (1, user latent vector)

val mid = myitemMatrix.filter(x => x._1==input_movie).map(x => (1, x._2))      // (1, item latent vector)

//(1, (user latent vector, item latent vector))
val res = uid.join(mid).map(x => x._2._1 * x._2._2).reduce(_+_)

println(s"Rating of user$input_user and movie$input_movie is $res.")



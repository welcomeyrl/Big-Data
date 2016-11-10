spark-shell --master local[*]
val lines = sc.textFile("hdfs://cshadoop1/yelpdatafall/review/review.csv")
val ratingSums = lines.map(line => line.split("\\^")).map(line=>(line(2),line(3).toDouble)).reduceByKey(_+_)
val counts = lines.map(line => line.split("\\^")).map(line => (line(2),1)).reduceByKey(_+_)
val avgs =  ratingSums.join(counts).mapValues{ case(sum, count) => (1.0*sum)/count}.map(x => x.swap)
val maxRating = avgs.sortByKey(false,1).map(x=>x._1).first()  
val review = avgs.filter( x => x._1 == maxRating).map(x => x.swap)
val data = sc.textFile("hdfs://cshadoop1/yelpdatafall/business/business.csv")
val business = data.map(d => d.split("\\^"))
val mappedBusiness = business.map(x => (x(0), x))
val joinLists = mappedBusiness.join(review)
val sortLists = joinLists.map(x=>(x._1,x._2._1.mkString(" "))).distinct.sortByKey(true,1).map(x=> x._2)
val output = sortLists.take(10) 
sc.parallelize(output).saveAsTextFile("hdfs://cshadoop1/rxy121130/HW2/Q3a")







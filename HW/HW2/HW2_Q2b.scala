spark-shell --master yarn-client --executor-memory 4G --executor-cores 7 --num-executors 6
val lines = sc.textFile("hdfs://cshadoop1/yelpdatafall/review/review.csv")
val ratingSums = lines.map(line => line.split("\\^")).map(line=>(line(2),line(3).toDouble)).reduceByKey(_+_)
val counts = lines.map(line => line.split("\\^")).map(line => (line(2),1)).reduceByKey(_+_)
val avgs =  ratingSums.join(counts).mapValues{ case(sum, count) => (1.0*sum)/count}.sortByKey(true,1).map(x => x.swap)
val maxRating = avgs.sortByKey(false,1).map(x=>x._1).first() 
val businessId = avgs.filter( x => x._1 == maxRating).map(x => x.swap).sortByKey(true, 1).take(10).map(x => x._1)
sc.parallelize(businessId).saveAsTextFile("hdfs://cshadoop1/rxy121130/HW2/Q2b")






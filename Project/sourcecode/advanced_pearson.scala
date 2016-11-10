import scala.collection.mutable.ListBuffer

def get_attribute(x : Array[String] ,y: Array[Array[String]]) : List[Double] = {
	val size = y.size
	var attribute=new ListBuffer[Double]();
	for (i <- 0 until size) 
	{
      		var sign=0;
		for(j <- 0 until x.size)
		{
			if (y(i).contains(x(j))) 
			{
				sign=1
   			}
		}
		if (sign==1)
		{
			attribute += (1.0).toDouble;
		}
		else
		{
			attribute += (0).toDouble;
		}
		
 	}
	return attribute.toList
}

def pearson(vector_x : List[Double] ,vector_y: List[Double]) : Double = {
	val psize = vector_x.length;
	var x_mean: Double = 0;
	var y_mean: Double = 0;
	for (i <- 0 until psize) 
	{
		x_mean += vector_x(i)
		y_mean += vector_y(i)
	}
	x_mean = x_mean/psize
	y_mean = y_mean/psize
	var v_1: Double =0;
	var v_2: Double =0;
	var v_3: Double =0;
	var sign=0;
	for (i <- 0 until psize)
	{
		if(vector_x(i)>0 && vector_y(i)>0)
		{
			//println(vector_x(i)+" "+vector_y(i))	
			v_1=v_1+(vector_x(i)-x_mean)*(vector_y(i)-y_mean)
			v_2=v_2+(vector_x(i)-x_mean)*(vector_x(i)-x_mean)
			v_3=v_3+(vector_y(i)-y_mean)*(vector_y(i)-y_mean)
			sign=1;
		}

 	}
	v_2 =Math.sqrt(v_2)
	v_3 =Math.sqrt(v_3)
	//println(v_1)
	//println(v_2)
	//println(v_3)
	var output: Double =0;
 	if(sign==1)
	{
		output= v_1/(v_2*v_3)
	}
	else
	{
		output= 0
	}
	return output
}


val data = sc.textFile("hdfs://cshadoop1.utdallas.edu/xxz126730/project/twiter_5000_10000_new.dat").map(t => (t.split(" ",2)(0), t.split(" ",2)(1).split(' ') )).cache()
val clusters = sc.textFile("/gxt140030/clusters.txt").map(t => (t.split(" "))).collect
val new_attribute = data.map(t => (t._1,get_attribute(t._2,clusters)))

println("Please input user_ID")
val movie =readLine()









val temp = new_attribute.filter(p => (p._1.equals(movie))).map(p=> p._2).collect;
val t_vector: List[Double] = temp(0);
val ids = new_attribute.filter(p => (!p._1.equals(movie))).map(p => (p._1, pearson(p._2,t_vector))).sortBy(p =>(-p._2,p._1)).take(5).map(p => p._1)
println("========================================================================");
ids.foreach(println);

//val temp = data.filter(p => (p._1.equals(movie))).map(p=> p._2).collect;
//val t_vector: Vector = temp(0);
//data.filter(p => (p._1.equals("1349"))).map(p => (p._1, pearson(p._2,t_vector))).collect;




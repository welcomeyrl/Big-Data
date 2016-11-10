import org.apache.spark.util.Vector

def pearson(vector_x : Vector ,vector_y: Vector) : Double = {
 val psize = vector_x.length;

 var v_1: Double =0;
 var v_2: Double =0;

 var sign=0;
 for (i <- 0 until psize) {
		v_1=v_1+Math.min(vector_x(i),vector_y(i))
		v_2=v_2+Math.max(vector_x(i),vector_y(i))
 }

 //println(v_1)
 //println(v_2)
 //println(v_3)
if(v_1/v_2>0)
{
	//println(v_1/v_2)
}

return v_1/v_2
}

val data = sc.textFile("hdfs://cshadoop1.utdallas.edu/xxz126730/project/twiter_5000_10000.dat").map(t => (t.split(" ",2)(0), Vector(t.split(" ",2)(1).split(' ').map(_.toDouble)) )).cache()

println("Please input user_ID")
val movie =readLine()





val temp = data.filter(p => (p._1.equals(movie))).map(p=> p._2).collect;
val t_vector: Vector = temp(0);
val ids = data.filter(p => (!p._1.equals(movie))).map(p => (p._1, pearson(p._2,t_vector))).sortBy(p =>(-p._2,p._1)).take(5).map(p => p._1)
//val ids = data.filter(p => (!p._1.equals(movie))).map(p => (p._1, pearson(p._2,t_vector))).sortBy(p =>(-p._2)).collect
println("========================================================================");
ids.foreach(println);

//val temp = data.filter(p => (p._1.equals(movie))).map(p=> p._2).collect;
//val t_vector: Vector = temp(0);
//data.filter(p => (p._1.equals("1349"))).map(p => (p._1, pearson(p._2,t_vector))).collect;




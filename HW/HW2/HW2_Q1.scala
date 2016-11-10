val input = readLine()

val lines = sc.textFile("hdfs://cshadoop1/yelpdatafall/business/business.csv")

lines.filter(line => line.contains(input)).map(line => line.split("\\^")).map(line => line(0)).distinct.saveAsTextFile("hdfs://cshadoop1/rxy121130/HW2/Q1")




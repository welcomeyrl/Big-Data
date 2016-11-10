import java.awt.List;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Top10{
	
	private static final String OUTPUT_PATH = "intermediate_output";
	

	public static class Map1 extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from ratings
			
			String[] mydata = value.toString().split("\\^");
		//	if (mydata.length > 23 && "review".compareTo(mydata[22]) == 0 ){
					
					context.write(new Text(mydata[2]),new FloatWritable(Float.parseFloat(mydata[3])));
		//	}	
					
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
		
		
		}
	}
	
    public static class Map2 extends Mapper<LongWritable, Text, FloatWritable, Text>{
    	
        private TreeMap<Text, Float> rate = new TreeMap<Text, Float>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from ratings
			int n = 0;
			String[] mydata = value.toString().trim().split("\t");
			rate.put(new Text(mydata[0]), Float.parseFloat(mydata[1]));
			
			//if(rate.size() > 10){
				//rate.remove(rate.firstKey());
			//}
			
			//context.write(NullWritable.get(), value);
		}
			
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			
			for ( Text r : rate.keySet() ) {

				context.write(new FloatWritable(rate.get(r)), r);
				
			}
		}
		
	
				
					
		}

			
	
	
	

	public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		
		private FloatWritable result = new FloatWritable();
	
		public void reduce(Text key, Iterable<FloatWritable> values,Context context ) throws IOException, InterruptedException {
		
			int count=0;
			float sum = 0;
			float ave = 0;
			
			for(FloatWritable t : values){
				
			    sum += t.get();
			    count++;
			}
		    
		   ave = sum/count;
		   
		   result.set(ave);
		    
	
			
			context.write(new Text(key), result);
			
		}
	}
	
   
	public static class Reduce2 extends Reducer<FloatWritable,Text, Text, NullWritable> {
    	
		private Map<String, Float> rate = new HashMap<>();
		
	
		public void reduce(FloatWritable key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
		
			
			for(Text t : values){
				
				//String[] mydata = t.toString().trim().split("\t");
				rate.put(t.toString(), Float.parseFloat(key.toString()));
			
			}
			
			
			
		}
		
		
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			Map<String, Float> sortedMap = sortByValues(rate);
			
			int counter = 0;
			/**
			for ( Text r : sortedMap.keySet() ) {

				if (counter++ == 10) {
                    break;
                }
				
				//context.write(new Text(r), NullWritable.get());
				
				
			}
			**/
			
			for(Map.Entry<String, Float> entry : sortedMap.entrySet()){
				String key = entry.getKey();
				Float value = entry.getValue();
				if (counter++ == 10) {
                    break;
                }
				//System.out.println(key + "\t" + value);
				context.write(new Text(key), NullWritable.get());
			}
		}
		
		
		
	}
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        LinkedList<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	public static TreeMap<String, Float> SortByValue 
	(HashMap<String, Float> map) {
		ValueComparator vc =  new ValueComparator(map);
		TreeMap<String,Float> sortedMap = new TreeMap<String,Float>(vc);
		sortedMap.putAll(map);
		return sortedMap;
}
	

	

// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: Top10 <in> <out>");
			System.exit(2);
		}
		
	//	DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+ otherArgs[1]), conf);       
		
		//conf.set("movieid", otherArgs[3]);
		
		Job job = new Job(conf, "Top10");
		job.setJarByClass(Top10.class);
		
		
		
	   
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReduceTasks(0);
//		uncomment the following line to add the Combiner
//		job.setCombinerClass(Reduce.class);
		
		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(FloatWritable.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		//Wait till job completion
		job.waitForCompletion(true);
		
		Job job2 = new Job(conf, "Ratebusiness2");
		job2.setJarByClass(Top10.class);
		
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		
		// set output key type 
		job2.setOutputKeyClass(FloatWritable.class);
		// set output value type
		job2.setOutputValueClass(Text.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		
		
		
		
		
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

class ValueComparator implements Comparator<String> {
	 
    Map<String, Float> map;
 
    public ValueComparator(Map<String, Float> base) {
        this.map = base;
    }
 
    public int compare(String a, String b) {
        if (map.get(a) >= map.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys 
    }
}

	
	


import java.awt.List;
import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;





public class ReduceJoin {
	
	private static final String OUTPUT_PATH = "intermediate_output1";
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
		        throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split("\\^");
			context.write(new Text(mydata[2]), new Text("review\t" + mydata[3]));   //output the businessid and rating from review.csv
			
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
		        throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split("\\^");
			context.write(new Text(mydata[0]), new Text("business\t" + mydata[1] + "\t" + mydata[2]));   //output the businessid and fulladdress and category from business.csv
			
		}
	}
	
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
		        throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split("\t");
			context.write(new Text(mydata[0]), new Text(mydata[1] + "\t" + mydata[2]));   //output the businessid, review and rating from reduce1
			
		}
	}
	
	
	public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
		        throws IOException, InterruptedException {
			
			String[] mydata =  value.toString().split("\\^");
		    context.write(new Text(mydata[0]), new Text("business\t" + mydata[1] + "\t" + mydata[2]));
			
			
		}
		
	}
	
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		
		private Map<String, Float> rate = new HashMap<>();
		private Map<String, Integer> map = new HashMap<>();
		
		private FloatWritable result = new FloatWritable();
	
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
		
			int count=0;
			float sum = 0;
			float ave = 0;
			
			for(Text t : values){
				String[] mydata = t.toString().split("\t");
				if(mydata[0].equals("review")){
					sum += Float.parseFloat(mydata[1].toString());
				    count++;
				}else{
					map.put(key.toString(), 1);
					//context.write(key, new Text(mydata[0] + "\t" + mydata[1] + "\t" + mydata[2]));
				}
			    
			}
		    
		    
		   
		   //result.set(ave);
		    
	
			
			//context.write(new Text(key), result);
			if(map.get(key.toString()) != null && count != 0){
				ave = sum/count;
				rate.put(key.toString(), ave);
			}
			
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			Map<String, Float> sortedMap = sortByValues(rate);
			
			
			
			int counter = 0;
			/**
			for (  r : sortedMap.keySet() ) {

				if (counter++ == 10) {
                    break;
                }
				
				context.write(r, new Text("Review\t" + sortedMap.get(r).toString()));
				
				
				
			}
			**/
			
			for(Map.Entry<String, Float> entry : sortedMap.entrySet()){
				String key = entry.getKey();
				Float value = entry.getValue();
				if (counter++ == 10) {
                    break;
                }
				//System.out.println(key + "\t" + value);
				context.write(new Text(key), new Text("Review\t" + String.valueOf(value)));
			}
		}
	}
    
    
    public static class Reduce2 extends Reducer<Text, Text ,Text, Text> {
    	private Map<Text, FloatWritable> rate = new HashMap<>();
		private Map<Text, String> business = new HashMap<>();
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			
			Text info = new Text();
			Text rating = new Text();
			for(Text t : values){
				String[] mydata = t.toString().split("\t");
				if(mydata[0].equals("Review")){
					rate.put(key, new FloatWritable(Float.parseFloat(mydata[1])));
				}else if(mydata[0].equals("business")){
					business.put(key, mydata[1] + "\t" + mydata[2]);
				}
			}
			
			if(rate.get(key) != null){
				context.write(key, new Text(business.get(key) + "\t" + rate.get(key).toString()));
			}

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			
			
			
			int counter = 0;
			for ( Text r : rate.keySet() ) {

				if (counter++ == 10) {
                    break;
                }
				
				//context.write(r, new Text("Review\t" + business.get(r) + "\t" + rate.get(r).toString()));
				
				
				
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

	
	// Driver program
			public static void main(String[] args) throws Exception {
				
				Configuration conf = new Configuration();
				String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
				if (otherArgs.length != 3) {
					System.err.println("Usage: ReduceJoin <review> <business> <out>");
					System.exit(2);
				}
				
				Job job = new Job(conf, "ReduceSideJoin1");
				
				job.setJarByClass(ReduceJoin.class);
				
				//job.setMapperClass(Map1.class);   
				job.setReducerClass(Reduce1.class);
				
				
				// set output key type 
				job.setOutputKeyClass(Text.class);
				// set output value type
				job.setOutputValueClass(Text.class);
				
				MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Map1.class);
				MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, Map2.class);
				
				//job.setNumReduceTasks(0);
				
				//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
				
				FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
				
				//Wait till job completion
				job.waitForCompletion(true);
				
				Job job2 = new Job(conf, "ReduceJoin2");
				job2.setJarByClass(ReduceJoin.class);
				
				//job2.setMapperClass(Map3.class);
				job2.setReducerClass(Reduce2.class);
				//job2.setNumReduceTasks(0);
				// set output key type 
				job2.setOutputKeyClass(Text.class);
				// set output value type
				job2.setOutputValueClass(Text.class);
				
				MultipleInputs.addInputPath(job2, new Path(OUTPUT_PATH), TextInputFormat.class, Map3.class);
				MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, Map4.class);
				
				//set the HDFS path of the input data
				//FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
				// set the HDFS path for the output 
				FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
				
				//Wait till job completion
				System.exit(job2.waitForCompletion(true) ? 0 : 1);
}
}

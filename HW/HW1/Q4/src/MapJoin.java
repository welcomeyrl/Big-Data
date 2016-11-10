
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapJoin {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> map = new HashMap<>();
        //Distributed cache does not work on the cluster, pls use thiscode for the setup phase instead.
        
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
            	String[] input = value.toString().split("\\^");
            	String userid = input[1];
            	String businessid = input[2];
            	String stars = input[3];
            	String address = map.get(businessid);
            	if(address.contains("Stanford") == true){
            		context.write(new Text(userid), new Text(stars));
            	}
        }
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //read data to memory on the mapper.
     //       List myCenterList = new ArrayList<>();
            Configuration conf = context.getConfiguration();
            String myfilepath = conf.get("myFilePath");
            //e.g /user/hue/input/
            Path part=new Path("hdfs://cshadoop1" + myfilepath);//Location of file in HDFS


            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
      //      context.write(new Text("read line"), new Text(fss[0].toString()));

            for (FileStatus status : fss) {
                Path pt = status.getPath();

                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                    System.out.println(line);
                    //do what you want with the line read
                    String[] values = line.split("\\^");
//                    context.write(new Text("read line"), new Text(String.valueOf(values.length)+"__"+values[0]));
                    map.put(values[0],values[1]);
                    line=br.readLine();
                    System.err.println(line);
                }

            }
        
        }


    }

    
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=3){
            System.err.println("Error! Insufficient arguments. Provide arguments <Input file path:businesses> <input directory:reviews> <Output directory>");
            System.exit(2);
        }
        conf.set("myFilePath", otherArgs[0]);
        Job job=new Job(conf, "MapJoin");
        job.setJarByClass(MapJoin.class);
       
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
   //     FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        job.waitForCompletion(true);
    }

}
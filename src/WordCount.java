import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

		public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
			
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
				
				//Convert text to string
				String line = value.toString();
				
				//Tokenizer will separate value based on spaces between them
				StringTokenizer tokenizer = new StringTokenizer(line);
				
				while(tokenizer.hasMoreTokens()){
					
					//Value will be set according to each word 
					value.set(tokenizer.nextToken());
					
					//Write each word along with a 1
					context.write(value, new IntWritable(1));
					
				}
			}
		}
		public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
				public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
					
					int sum = 0;
					
					for(IntWritable x : value){
						sum += x.get();
					}
					
					context.write(key, new IntWritable(sum));
				}
		}
		public static void main(String args[]) throws Exception{
			
			//Create a configuration to setup/define jobs
			Configuration conf = new Configuration();
			
			//Define main job
			Job job = Job.getInstance(conf, "WordCount");
			
			//Set class names for job
			job.setJarByClass(WordCount.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			//Set output class names
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//Set format of input and output
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			Path outputPath = new Path(args[1]);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			outputPath.getFileSystem(conf).delete(outputPath, true);
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		}
}

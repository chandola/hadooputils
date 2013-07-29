package gov.ornl.cms.finalaction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IndexGenerator
{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String [] tokens = value.toString().split(LocalConfiguration.delim);
			Configuration config = context.getConfiguration();
			StringBuffer buf = new StringBuffer();
			int numFields = config.getInt("numFields",0);
			for(int i = 0; i < numFields; i++)
				buf.append(tokens[config.getInt("field"+i, 0)]);
			
			int hashKey = buf.toString().hashCode();
			context.write(new IntWritable(hashKey),new IntWritable(0));
		}
	} 
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
	{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, null);
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		if(args.length < 4)
		{
			System.err.println("Incorrect Arguments");
			System.err.println("USAGE:\n java IndexGenerator inputdir outputdir numfields field1 field2 field3 ... ");
			return;
		}
		Configuration config = new Configuration();

		Path inputDir = new Path(args[0]);			
		Path outputDir = new Path(args[1]);
		int numFields = Integer.parseInt(args[2]);
		if(args.length != numFields + 3)
		{
			System.err.println("Insufficient Arguments. Need "+numFields+" field positions");
			return;
		}
		config.setInt("numFields", numFields);
		for(int i = 0; i < numFields; i++)		
			config.setInt("field"+i,Integer.parseInt(args[i+3]));
		

		Job job = new Job(config, "IndexGenerator");
		job.setJarByClass(IndexGenerator.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}
}
					

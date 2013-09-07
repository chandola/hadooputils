package gov.ornl.cms.finalaction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Joins partial final action claim records and overflow records
 * 
 * @author chandola
 */
public class PartialFinalActionClaimsOverflowJoiner
{

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		private ArrayList<String> index;
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			index = new ArrayList<String>();
			//read the index file
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			FileStatus[] files = fs.globStatus(new Path(config.get("indexFile") + "/part-*"));
			for(FileStatus f: files)
			{
				try
				{
					FSDataInputStream in = fs.open(f.getPath());
					BufferedReader bIn = new BufferedReader(new InputStreamReader(in));
					String s;
					s = bIn.readLine();
					while(s != null)
					{
						index.add(s.trim());
						s = bIn.readLine();
					}
				}
				catch(Exception e)
				{
					//do nothing
				}
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] tokens = value.toString().split(LocalConfiguration.delim);
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			String output;
			String claimId;
			boolean isOverflow = true;
			//this is an overflow record
			if(fileName.indexOf("overflows") != -1)
			{
				isOverflow = true;
				claimId = tokens[LocalConfiguration.overflowsClaimIdIndex];
				if(!index.contains((new Integer(claimId.hashCode())).toString()))
					return;
				output = tokens[LocalConfiguration.overflowsActionCodeIndex];
			}
			else
			{
				//this is a partial final action claims record
				claimId = tokens[4];
				output = value.toString();
			}
			if(isOverflow)
				context.write(new Text(claimId), new Text("O::"+output));
			else
				context.write(new Text(claimId), new Text("F::"+output));
		}
	} 

	public static class Reduce extends Reducer<Text, Text, LongWritable, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String partialActionClaim = null;
			String actionCode = null;
			
			for(Text t: values)
			{
				String[] s = t.toString().split("::");
				if(s[0].compareToIgnoreCase("F") == 0)
				{
					partialActionClaim = s[1];
				}
				else
				{
					if(actionCode == null)
						actionCode = s[1];
				}
				if(actionCode != null && partialActionClaim != null)
					break;
			}
			String joinedRecord = partialActionClaim+LocalConfiguration.delim+actionCode;
			context.write(null,new Text(joinedRecord));
		}
	}

	public static void main(String[] args) throws Exception
	{
		if(args.length < 4)
		{
			System.err.println("Incorrect Arguments");
			System.err.println("USAGE:\n java PartialFinalActionClaimsOverflowJoiner partialfinalactionclaimsdir overflowclaimsdir indexfile outputdir");
			return;
		}
		Configuration config = new Configuration();
		config.set("indexFile", args[2]);
		Job job = new Job(config, "PartialFinalActionClaimsOverflowJoiner");
		job.setJarByClass(PartialFinalActionClaimsOverflowJoiner.class);
		//add partial final action claim files
		FileInputFormat.addInputPath(job,new Path(args[0]));
		//add overflows dir		
		FileStatus[] files;
		files = FileSystem.get(config).globStatus(new Path(args[1]));
		for(FileStatus fs: files)
			FileInputFormat.addInputPath(job, fs.getPath());
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
	}
}

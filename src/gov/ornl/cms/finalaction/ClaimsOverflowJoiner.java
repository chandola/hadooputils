package gov.ornl.cms.finalaction;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
 * Joins claim records and overflow records and extracts relevant fields for final action processing
 * 
 * @author chandola
 */
public class ClaimsOverflowJoiner
{

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] tokens = value.toString().split(LocalConfiguration.delim);
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			StringBuffer buf = new StringBuffer();
			boolean isClaim = false;
			String claimId = "";
			if(fileName.indexOf("claims") != -1)
			{
				//this is a claims record
				isClaim = true;
				claimId = tokens[LocalConfiguration.claimIdIndex];
				buf.append(tokens[LocalConfiguration.hicIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.provNumberIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.fromDateIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.thruDateIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.processingDateIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.queryCodeIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.icnIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.totalChargesIndex]);
			}
			else
			{
				//this is an overflow record
				if(fileName.indexOf("overflows") != -1)
				{
					claimId = tokens[LocalConfiguration.overflowsClaimIdIndex];
					buf.append(tokens[LocalConfiguration.overflowsActionCodeIndex]);
				}
			}
			if(isClaim)
				context.write(new Text(claimId), new Text("C::"+buf.toString()));
			else
				context.write(new Text(claimId), new Text("O::"+buf.toString()));
		}
	} 

	public static class Reduce extends Reducer<Text, Text, LongWritable, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Vector<String> groupClaims = new Vector<String>();
			String actionCode = null;
			for(Text t: values)
			{
				String[] s = t.toString().split("::");
				if(s[0].compareToIgnoreCase("C") == 0)
				{
					groupClaims.add(s[1]);
				}
				else
				{
					if(actionCode == null)
						actionCode = s[1];
				}
			}
			String joinedRecord = key.toString()+LocalConfiguration.delim+groupClaims.get(0)+LocalConfiguration.delim+actionCode;
			context.write(null,new Text(joinedRecord));
		}
	}

	public static void main(String[] args) throws Exception
	{
		if(args.length < 3)
		{
			System.err.println("Incorrect Arguments");
			System.err.println("USAGE:\n java ClaimsOverflowJoiner inputrootdir inputfilepattern outputrootdir");
			return;
		}
		Configuration config = new Configuration();
		

		FileStatus[] files;
		//process claim files
		String inputRootDir = args[0];
		if(inputRootDir.endsWith(Path.SEPARATOR))
			inputRootDir = inputRootDir.substring(0,inputRootDir.length()-1);
		files = FileSystem.get(config).globStatus(new Path(inputRootDir+Path.SEPARATOR+args[1]));
		for(FileStatus fs: files)
		{
			Job job = new Job(config, "ClaimsOverflowJoiner");
			job.setJarByClass(ClaimsOverflowJoiner.class);
			FileInputFormat.addInputPath(job, fs.getPath());
			String outputDir = args[2]+fs.getPath().toString().substring(inputRootDir.length()+1);			
			Path outputDirPath = new Path(outputDir);
			FileSystem.mkdirs(FileSystem.get(config), outputDirPath,FsPermission.valueOf("-rwxrwxrw-"));
			FileOutputFormat.setOutputPath(job, outputDirPath);

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
}

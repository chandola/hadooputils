package gov.ornl.cms.finalaction;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartialFinalActionExecutor 
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
			String hashString = "";
			StringBuffer buf = new StringBuffer();
			StringBuffer buf1 = new StringBuffer();
			boolean isClaim = false;
			if(fileName.indexOf("claims") != -1)
			{
				//this is a claims record
				isClaim = true;
				hashString += tokens[LocalConfiguration.hicIndex];
				hashString += tokens[LocalConfiguration.provNumberIndex];
				hashString += tokens[LocalConfiguration.fromDateIndex];
				hashString += tokens[LocalConfiguration.thruDateIndex];
				buf.append(tokens[LocalConfiguration.hicIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.provNumberIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.fromDateIndex]+LocalConfiguration.delim);
				buf.append(tokens[LocalConfiguration.thruDateIndex]);
				buf1.append(tokens[LocalConfiguration.claimIdIndex]+LocalConfiguration.delim);
				buf1.append(tokens[LocalConfiguration.queryCodeIndex]+LocalConfiguration.delim);
				buf1.append(tokens[LocalConfiguration.processingDateIndex]);
			}
			else
			{
				//this is a final action record
				hashString += tokens[0];
				hashString += tokens[1];
				hashString += tokens[2];
				hashString += tokens[3];
				buf.append(tokens[0]+LocalConfiguration.delim);
				buf.append(tokens[1]+LocalConfiguration.delim);
				buf.append(tokens[2]+LocalConfiguration.delim);
				buf.append(tokens[3]);
				buf1.append("F");
			}
			int hashKey = hashString.hashCode();
			if(index.contains(new String((new Integer(hashKey)).toString())))
			{
				if(isClaim)
					context.write(new Text(buf.toString()),new Text("C::"+buf1.toString()));
				else
					context.write(new Text(buf.toString()),new Text("F::"+buf1.toString()));
			}
		}
	} 

	public static class Reduce extends Reducer<Text, Text, IntWritable, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Vector<String> groupClaims = new Vector<String>();
			boolean newGroup = true;
			for(Text t: values)
			{
				String[] s = t.toString().split("::");
				if(s[0].compareToIgnoreCase("C") == 0)
				{
					groupClaims.add(s[1]);
				}
				else
				{
					newGroup = false;
				}
			}

			//Apply the final actions logic
			//Extract relevant fields from all claims
			HashSet<String> queryCodeSet = new HashSet<String>();
			Vector<String> queryCodes = new Vector<String>(groupClaims.size());
			Vector<Integer> nchProcessingDates = new Vector<Integer>(groupClaims.size());
			Vector<String> claimIds = new Vector<String>(groupClaims.size());
			//might need ICN field later

			for(String c: groupClaims)
			{
				//find the query code
				String [] tokens = c.split(LocalConfiguration.delim);
				claimIds.add(tokens[0]);
				queryCodes.add(tokens[1]);
				queryCodeSet.add(tokens[1]);
				nchProcessingDates.add(new Integer(tokens[2]));
			}

			//prepare the partial final action string -- groupfields, claimid, date
			String [] tokens = key.toString().split(LocalConfiguration.delim);
			String partialFinalActionString = "";

			partialFinalActionString += tokens[LocalConfiguration.hicIndex]+",";
			partialFinalActionString += tokens[LocalConfiguration.provNumberIndex]+",";
			partialFinalActionString += tokens[LocalConfiguration.fromDateIndex]+",";
			partialFinalActionString += tokens[LocalConfiguration.thruDateIndex]+",";

			if(queryCodeSet.size() == 1 && 
					queryCodes.get(0).compareTo("3") == 0)
			{
				//if this is an old claim group then skip
				//else create a new partial final actions record
				if(newGroup)
				{
					String claimId = claimIds.get(0);
					String finalActionString = partialFinalActionString+
							claimId+LocalConfiguration.delim+
							queryCodes.get(0)+LocalConfiguration.delim+
							nchProcessingDates.get(0);
					context.write(null,new Text(finalActionString));
				}
			}
			else
			{
				if(queryCodeSet.size() == 1)
				{
					//if all the claims in the group have same query code take the latest date
					int latestDateIndex = 0;
					Integer current = nchProcessingDates.get(0);
					int ind = 0;
					for(Integer d: nchProcessingDates)
					{
						if(d > current)
						{
							current = d;
							latestDateIndex = ind;
						}
						ind++;
					}
					//create a new final actions record for the latest date claim
					String claimId = claimIds.get(latestDateIndex);
					String finalActionString = partialFinalActionString+
							claimId+LocalConfiguration.delim+
							queryCodes.get(latestDateIndex)+LocalConfiguration.delim+
							nchProcessingDates.get(latestDateIndex);
					context.write(null,new Text(finalActionString));
				}
				else
				{
					//write out all the claims in this group
					for(int i = 0; i < groupClaims.size(); i++)
					{
						String finalActionString = partialFinalActionString+
								claimIds.get(i)+LocalConfiguration.delim+
								queryCodes.get(i)+LocalConfiguration.delim+
								nchProcessingDates.get(i);
						context.write(null,new Text(finalActionString));
					}
				}
			}
		}

	}

	public static void main(String[] args) throws Exception 
	{
		if(args.length < 4)
		{
			System.err.println("Incorrect Arguments");
			System.err.println("USAGE:\n java PartialFinalActionExecutor claimsdir finalactionclaimsdir indexfilesdir outputdir");
			return;
		}
		Configuration config = new Configuration();
		Path outputDir = new Path(args[3]);
		config.set("indexFile",args[2]);
		Job job = new Job(config, "PartialFinalActionExecutor");
		job.setJarByClass(PartialFinalActionExecutor.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileStatus[] files;
		//add claim files
		files = FileSystem.get(config).globStatus(new Path(args[0]));
		for(FileStatus fs: files)
			FileInputFormat.addInputPath(job, fs.getPath());
		//add final action claim files
		files = FileSystem.get(config).globStatus(new Path(args[1]));
		for(FileStatus fs: files)
			FileInputFormat.addInputPath(job, fs.getPath());
		FileOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}
}


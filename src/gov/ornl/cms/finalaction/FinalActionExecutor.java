package gov.ornl.cms.finalaction;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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

public class FinalActionExecutor 
{
	private static String delim = ",";
	private static int hicIndex = 10;
	private static int provNumberIndex = 25;
	private static int fromDateIndex = 15;
	private static int thruDateIndex = 16;
	
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
			String [] tokens = value.toString().split(delim);
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			StringBuffer buf = new StringBuffer();
			boolean isClaim = false;
			boolean isOverflow = false;
			if(fileName.indexOf("claims") != -1)
			{
				//this is a claims record
				isClaim = true;
				buf.append(tokens[hicIndex]);
				buf.append(tokens[provNumberIndex]);
				buf.append(tokens[fromDateIndex]);
				buf.append(tokens[thruDateIndex]);
			}
			else
			{
				if(fileName.indexOf("overflows") != -1)
				{
					isOverflow = true;
					buf.append(tokens[hicIndex]);
					buf.append(tokens[provNumberIndex]);
					buf.append(tokens[fromDateIndex]);
					buf.append(tokens[thruDateIndex]);
				}
				else
				{
					//this is a final action record
					Configuration config = context.getConfiguration();
					int numKeys = config.getInt("numKeys",0);
					for(int i = 0; i < numKeys; i++)
						buf.append(tokens[config.getInt("fKey"+i,0)]);
				}
			}
			int hashKey = buf.toString().hashCode();
			if(index.contains(new String((new Integer(hashKey)).toString())))
			{
				if(isClaim)
					context.write(new Text("C::"+buf.toString()),value);
				else if(isOverflow)
					context.write(new Text("O::"+buf.toString()),value);
				else
					context.write(new Text("F::"+buf.toString()),value);
			}
		}
	} 

	public static class Reduce extends Reducer<Text, Text, IntWritable, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String delim = config.get("delim");

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
			HashSet<String> queryCodes = new HashSet<String>();
			Vector<Integer> nchProcessingDates = new Vector<Integer>(groupClaims.size());
			Vector<String> claimActionCodes = new Vector<String>(groupClaims.size());
			Vector<String> claimIds = new Vector<String>(groupClaims.size());


			int queryCodeIndex = config.getInt("queryCodeIndex", 0);
			int nchProcessingDateIndex = config.getInt("nchProcessingDateIndex",0);
			int claimActionCodeIndex = config.getInt("claimActionCodeIndex", 0);
			int claimIdIndex = config.getInt("claimIdIndex",0);
			for(String c: groupClaims)
			{
				//find the query code
				String [] tokens = c.split(delim);
				queryCodes.add(tokens[queryCodeIndex]);
				nchProcessingDates.add(new Integer(tokens[nchProcessingDateIndex]));
				claimIds.add(tokens[claimIdIndex]);
				claimActionCodes.add(tokens[claimActionCodeIndex]);
			}
			//prepare the final action string
			String [] tokens = groupClaims.get(0).split(delim);
			String partialFinalActionString = new String();
			String date = config.get("date");
			int numKeys = config.getInt("numKeys",0);
			partialFinalActionString += date;
			for(int i = 0; i < numKeys; i++)
			{
				int index = config.getInt("cKey"+i,0);
				partialFinalActionString += ","+tokens[index];
			}
			Iterator<String> iterator = queryCodes.iterator();

			if(queryCodes.size() == 1 && 
					iterator.next().compareTo("3") == 0)
			{
				//if this is an old claim group then skip
				//else create a new final actions record
				if(newGroup)
				{
					String claimId = claimIds.get(0);
					String finalActionString = claimId+","+partialFinalActionString;
					context.write(null,new Text(finalActionString));
				}
			}
			else
			{
				if(queryCodes.size() == 1)
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
					String finalActionString = claimId + "," + partialFinalActionString;
					context.write(null,new Text(finalActionString));
				}
				else
				{
					//if there are different query codes check for credit debit pairs
					boolean [] toCancel = new boolean[claimActionCodes.size()];
					for(int i = 0; i < claimActionCodes.size(); i++)
						toCancel[i] = false;
					//check for claimActionCodes 2 and 3 pairs
					Vector<Integer> indices2 = new Vector<Integer>();
					Vector<Integer> indices3 = new Vector<Integer>();
					int i = 0;
					for(String s: claimActionCodes)
					{
						if(s.compareTo("2") == 0)
							indices2.add(new Integer(i));
						if(s.compareTo("3") == 0)
							indices3.add(new Integer(i));
						i++;
					}
					int cnt = Math.min(indices2.size(), indices3.size());

					for(int j = 0; j < cnt; j++)
					{
						toCancel[indices2.get(j)] = true;
						toCancel[indices3.get(j)] = true;
					}
					Vector<String> restClaimActionCodes = new Vector<String>();
					Vector<Integer> restNchProcessingDates = new Vector<Integer>();
					for(int j = 0; j < claimActionCodes.size(); j++)
					{
						if(!toCancel[j])
						{
							restClaimActionCodes.add(claimActionCodes.get(j));
							restNchProcessingDates.add(nchProcessingDates.get(j));
						}
					}
					int latestDateIndex;
					latestDateIndex = findLatestIndex(restClaimActionCodes,restNchProcessingDates,"5");
					if(latestDateIndex == -1)
						latestDateIndex = findLatestIndex(restClaimActionCodes,restNchProcessingDates,"3");
					if(latestDateIndex == -1)
						latestDateIndex = findLatestIndex(restClaimActionCodes,restNchProcessingDates,"1");
					if(latestDateIndex != -1)
					{
						String claimId = claimIds.get(latestDateIndex);
						String finalActionString = claimId + "," + partialFinalActionString;
						context.write(null,new Text(finalActionString));
					}


				}
			}
		}

		private int findLatestIndex(Vector<String> codes, Vector<Integer> dates, String match)
		{
			int index = -1;
			int latestDate = -1;
			for(int i = 0; i < codes.size(); i++)
			{
				if(codes.get(i).compareTo(match) == 0)
				{
					if(index == -1)
					{
						latestDate = dates.get(i);
						index = i;
					}
					else
					{
						if(dates.get(i) > latestDate)
						{
							latestDate = dates.get(i);
							index = i;
						}
					}

				}
			}
			return index;
		}
	}

	public static void main(String[] args) throws Exception 
	{
		if(args.length < 6)
		{
			System.err.println("Incorrect Arguments");
			System.err.println("USAGE:\n java FinalActionExecutor claimsdir overflowsdir finalactionsdir outputdir indexfilesdir tmpdir");
			return;
		}
		Configuration config = new Configuration();
		Path outputDir = new Path(args[3]);
		config.set("indexFile",args[4]);
		Job job = new Job(config, "FinalActionExecutor");
		job.setJarByClass(FinalActionExecutor.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileStatus[] files;
		//add claim files
		files = FileSystem.get(config).globStatus(new Path(args[0]));
		for(FileStatus fs: files)
			FileInputFormat.addInputPath(job, fs.getPath());
		files = FileSystem.get(config).globStatus(new Path(args[1]));
		for(FileStatus fs: files)
			FileInputFormat.addInputPath(job, fs.getPath());
		files = FileSystem.get(config).globStatus(new Path(args[2]));
		for(FileStatus fs: files)
			FileInputFormat.addInputPath(job, fs.getPath());
		FileOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}
}


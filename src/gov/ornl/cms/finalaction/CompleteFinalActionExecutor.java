package gov.ornl.cms.finalaction;

import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

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

/**
 * Performs the third step of final action processing using the claim action field from the intermediate
 * final action claim records.
 * 
 * @author chandola
 *
 */
public class CompleteFinalActionExecutor 
{
	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] tokens = value.toString().split(LocalConfiguration.delim);
			StringBuffer buf = new StringBuffer();
			StringBuffer buf1 = new StringBuffer();

			buf.append(tokens[0]+LocalConfiguration.delim);
			buf.append(tokens[1]+LocalConfiguration.delim);
			buf.append(tokens[2]+LocalConfiguration.delim);
			buf.append(tokens[3]);
			buf1.append(tokens[4]+LocalConfiguration.delim);
			buf1.append(tokens[5]+LocalConfiguration.delim);
			buf1.append(tokens[6]+LocalConfiguration.delim);
			buf1.append(tokens[7]);
			context.write(new Text(buf.toString()),new Text(buf1.toString()));
		}

	} 

	public static class Reduce extends Reducer<Text, Text, IntWritable, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			Vector<String> groupClaims = new Vector<String>();
			for(Text t: values)
				groupClaims.add(t.toString());

			//Apply the final "final actions logic"
			//Extract relevant fields from all claims
			Vector<String> queryCodes = new Vector<String>();
			Vector<Integer> nchProcessingDates = new Vector<Integer>(groupClaims.size());
			Vector<String> claimIds = new Vector<String>(groupClaims.size());
			Vector<String> claimActionCodes = new Vector<String>(groupClaims.size());
			for(String c: groupClaims)
			{
				//find the query code
				String [] tokens = c.split(LocalConfiguration.delim);
				claimIds.add(tokens[0]);
				queryCodes.add(tokens[1]);
				nchProcessingDates.add(new Integer(tokens[2]));
				claimActionCodes.add(tokens[3]);
			}
			//prepare the partial final action string -- groupfields, claimid, date
			String [] tokens = key.toString().split(LocalConfiguration.delim);
			String partialFinalActionString = "";

			partialFinalActionString += tokens[LocalConfiguration.hicIndex]+LocalConfiguration.delim;
			partialFinalActionString += tokens[LocalConfiguration.provNumberIndex]+LocalConfiguration.delim;
			partialFinalActionString += tokens[LocalConfiguration.fromDateIndex]+LocalConfiguration.delim;
			partialFinalActionString += tokens[LocalConfiguration.thruDateIndex];

			String todayDate = config.get("todayDate");
			if(groupClaims.size() == 1)
			{
				String finalActionString = partialFinalActionString+LocalConfiguration.delim+
						claimIds.get(0)+LocalConfiguration.delim+
						todayDate;
				context.write(null, new Text(finalActionString));
			}
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
		if(args.length < 2)
		{
			System.err.println("Incorrect Arguments");
			System.err.println("USAGE:\n java CompleteFinalActionExecutor joinedpartialfinalactionclaimsdir outputdir");
			return;
		}
		Configuration config = new Configuration();
		//the current date

		SimpleDateFormat formatter = new SimpleDateFormat("YYYYDDD");
		Date date = new Date();
		config.set("todayDate",formatter.format(date));
		Path outputDir = new Path(args[1]);
		Job job = new Job(config, "CompleteFinalActionExecutor");
		job.setJarByClass(CompleteFinalActionExecutor.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//add joined partial final action claim files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}
}


package gov.ornl.hadoop.utils;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

/*
 * Convert a delimited text file to a Mahout sequence file format.
 * Usage:
 * Usage:                                                                          
 * [--input <input> --output <output> --delim <delim>]                            
 * Options                                                                         
 *   --input (-i) input      Path to job input directory.                          
 *   --output (-o) output    The directory pathname for output.                    
 *   --delim (-dl) delim     Delimiter for columns.  Default is , (<COMMA>)  
 * 
 * @author chandola
 */

public class TextToSequence extends AbstractJob
{

	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new TextToSequence(), args);
	}

	public static class Map extends Mapper<IntWritable, Text, IntWritable, VectorWritable> 
	{
		public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String delim = config.get("delim");
			int numCols = config.getInt("numCols",0);
			StringTokenizer tokenizer = new StringTokenizer(value.toString(),delim);
			int i = 0;
			DenseVector vector = new DenseVector(numCols);
			while(tokenizer.hasMoreTokens())
			{
				vector.set(i, Double.parseDouble(tokenizer.nextToken()));
				i++;
			}
			context.write(key,new VectorWritable(vector));
		}
	} 
	
	public static class Reduce extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>
	{
		
	}
	
	public int run(String [] args) throws Exception
	{
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
		ArgumentBuilder abuilder = new ArgumentBuilder();

		GroupBuilder gbuilder = new GroupBuilder();

		Option inputDirOpt = DefaultOptionCreator.inputOption().create();
		Option outputDirOpt = DefaultOptionCreator.outputOption().create();
		Option numColsOpt = obuilder.withLongName("numCols").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("numcols").create()).withDescription(
						"Number of columns.").withShortName("nc").create();
		Option delimOpt = obuilder.withLongName("delim").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("delim").create()).withDescription(
						"Delimiter for columns.  Default is , (<COMMA>)").withShortName("dl").create();
		Option helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h")
				.create();
		Group group = gbuilder.withName("Options").
				withOption(inputDirOpt).
				withOption(numColsOpt).
				withOption(outputDirOpt).
				withOption(delimOpt).
				create();

		try
		{
			Configuration config = new Configuration();

			Parser parser = new Parser();
			parser.setGroup(group);
			parser.setHelpOption(helpOpt);
			CommandLine cmdLine = parser.parse(args);
			if(cmdLine.hasOption(helpOpt))
			{
				CommandLineUtil.printHelp(group);
				return -1;
			}

			if(!cmdLine.hasOption(inputDirOpt) || !cmdLine.hasOption(outputDirOpt) || !(cmdLine.hasOption(numColsOpt)))
			{
				CommandLineUtil.printHelp(group);
				return -1;
			}
			int numCols = -1;
			if(cmdLine.hasOption(numColsOpt))
				numCols = Integer.parseInt(((String) cmdLine.getValue(numColsOpt)));
			String delim = ",";
			if(cmdLine.hasOption(delimOpt))
				delim = (String) cmdLine.getValue(delimOpt);
			config.setInt("numCols", numCols);
			config.set("delim", delim);
			Path inputDir = new Path((String) cmdLine.getValue(inputDirOpt));			
			Path outputDir = new Path((String) cmdLine.getValue(outputDirOpt));
			
			Job job = new Job(config, "TextToSequence");
			job.setJarByClass(TextToSequence.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setNumReduceTasks(0);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(VectorWritable.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(VectorWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.addInputPath(job, inputDir);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.waitForCompletion(true);

		} 
		catch (OptionException e) 
		{
			CommandLineUtil.printHelp(group);
		}

		return 0;

	}

}

package gov.ornl.hadoop.utils.spatial;

import java.net.URI;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.VectorWritable;


/**
 * Runs a Gaussian Process training tool on spatial data.
 * 
 * Usage:\                                                                          
 * [--input \<input\> --output \<output\> --numCols numcols --delim delimiter --props /<properties/>] <br> 
 * Options
 *   --input (-i) input             Path to job input directory<br>                   
 *   --output (-o) output           The directory pathname for output<br>             
 *   --numCols (-nc) numcols        Number of columns in input text file<br>                             
 *   --delim (-dl) delim            Delimiter for columns.  Default is \<SPACE\><br>  
 *   --props (-p) properties        Properties files		<br>                             
 * 
 * @author chandola
 */

public class ChangeDetection extends AbstractJob
{

	
	/**
	 * Runs the GPTrain tool.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new ChangeDetection(), args);
	}

	public int run(String [] args) throws Exception
	{
		Configuration config = getConf();
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
		ArgumentBuilder abuilder = new ArgumentBuilder();

		GroupBuilder gbuilder = new GroupBuilder();

		Option inputDirOpt = DefaultOptionCreator.inputOption().create();
		Option outputDirOpt = DefaultOptionCreator.outputOption().create();
		Option trainDirOpt = obuilder.withLongName("trainDir").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("trainDir").create()).withDescription(
						"Location of training model.").withShortName("t").create();
		Option monitorDirOpt = obuilder.withLongName("monitorDir").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("monitorDir").create()).withDescription(
						"Location of monitoring output.").withShortName("m").create();
		Option smoothDirOpt = obuilder.withLongName("smoothDir").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("smoothDir").create()).withDescription(
						"Location of smoothed output.").withShortName("s").create();
			Option numColsOpt = obuilder.withLongName("numCols").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("numcols").create()).withDescription(
						"Number of columns.").withShortName("nc").create();
		Option delimOpt = obuilder.withLongName("delim").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("delim").create()).withDescription(
						"Delimiter for columns.  Default is <SPACE>").withShortName("dl").create();
		Option propsOpt = obuilder.withLongName("props").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("props").create()).withDescription(
						"Location of properties file.").withShortName("p").create();
		Option helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h")
				.create();
		
		Group group = gbuilder.withName("Options").
				withOption(inputDirOpt).
				withOption(outputDirOpt).
				withOption(trainDirOpt).
				withOption(monitorDirOpt).
				withOption(smoothDirOpt).
				withOption(propsOpt).
				withOption(numColsOpt).
				withOption(delimOpt).
				create();
		
		try
		{
			Parser parser = new Parser();
			parser.setGroup(group);
			parser.setHelpOption(helpOpt);
			CommandLine cmdLine = parser.parse(args);
			if(cmdLine.hasOption(helpOpt))
			{
				CommandLineUtil.printHelp(group);
				return -1;
			}

			if(!cmdLine.hasOption(inputDirOpt)||
					!cmdLine.hasOption(outputDirOpt)||
					!cmdLine.hasOption(trainDirOpt)||
					!cmdLine.hasOption(monitorDirOpt)||
					!cmdLine.hasOption(smoothDirOpt)||
					!(cmdLine.hasOption(numColsOpt))||
					!(cmdLine.hasOption(propsOpt)))					
			{
				CommandLineUtil.printHelp(group);
				return -1;
			}
			int numCols = Integer.parseInt(((String) cmdLine.getValue(numColsOpt)));
			String delim = " +";
			if(cmdLine.hasOption(delimOpt))
				delim = (String) cmdLine.getValue(delimOpt);
			config.set("delim",delim);
			config.setInt("numCols", numCols);
			Path propsFile = new Path((String) cmdLine.getValue(propsOpt));			
			DistributedCache.setCacheFiles(new URI[] {propsFile.toUri()}, config);
			
			Path inputDir = new Path((String) cmdLine.getValue(inputDirOpt));			
			Path outputDir = new Path((String) cmdLine.getValue(outputDirOpt));
			Path trainDir = new Path((String) cmdLine.getValue(trainDirOpt));			
			Path monitorDir = new Path((String) cmdLine.getValue(monitorDirOpt));
			Path smoothDir = new Path((String) cmdLine.getValue(smoothDirOpt));			
			
			FileSystem fs = FileSystem.get(config);

			Job job;
			if(!fs.exists(trainDir))
			{
				//Task 1 - Train
				System.out.println("Executing GPTrain");
				job = new Job(config, "GPTrain");
				job.setJarByClass(GPTrain.class);

				job.setMapperClass(GPTrain.Map.class);
				job.setReducerClass(GPTrain.Reduce.class);

				job.setMapOutputKeyClass(IntPairWritable.class);
				job.setMapOutputValueClass(VectorWritable.class);
				job.setOutputKeyClass(IntPairWritable.class);
				job.setOutputValueClass(VectorWritable.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);

				FileInputFormat.addInputPath(job, inputDir);
				FileOutputFormat.setOutputPath(job, trainDir);
				job.waitForCompletion(true);
			}
			//Task 2 - Monitor
			if(!fs.exists(monitorDir))
			{
				System.out.println("Executing GPChange");
				Path modelFile = trainDir;
				DistributedCache.addCacheFile(modelFile.toUri(), config);
			
				job = new Job(config, "ChangeDetection");
				job.setJarByClass(GPChange.class);
		
				job.setMapperClass(GPChange.Map.class);
				job.setReducerClass(GPChange.Reduce.class);
		
				job.setMapOutputKeyClass(IntPairWritable.class);
				job.setMapOutputValueClass(VectorWritable.class);
				job.setOutputKeyClass(IntPairWritable.class);
				job.setOutputValueClass(VectorWritable.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
				FileInputFormat.addInputPath(job, inputDir);
				FileOutputFormat.setOutputPath(job, monitorDir);
				job.waitForCompletion(true);
			}
			//Task 3 - Smooth
			if(!fs.exists(smoothDir))
			{
				System.out.println("Executing TimeSeriesSmoother");
				job = new Job(config, "TimeSeriesSmoother");
				job.setJarByClass(TimeSeriesSmoother.class);
	
				job.setMapperClass(TimeSeriesSmoother.Map.class);
				job.setReducerClass(TimeSeriesSmoother.Reduce.class);
				job.setNumReduceTasks(0);
				job.setMapOutputKeyClass(IntPairWritable.class);
				job.setMapOutputValueClass(VectorWritable.class);
				job.setOutputKeyClass(IntPairWritable.class);
				job.setOutputValueClass(VectorWritable.class);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
	
				FileInputFormat.addInputPath(job, monitorDir);
				FileOutputFormat.setOutputPath(job, smoothDir);
				job.waitForCompletion(true);
			}
			//Task 4 - Aggregate
			if(!fs.exists(outputDir))
			{
				System.out.println("Executing TimeSeriesAggregator");
				job = new Job(config, "TimeSeriesAggregator");
				job.setJarByClass(TimeSeriesAggregator.class);
	
				job.setMapperClass(TimeSeriesAggregator.Map.class);
				job.setReducerClass(TimeSeriesAggregator.Reduce.class);
				job.setNumReduceTasks(0);
				job.setMapOutputKeyClass(IntPairWritable.class);
				job.setMapOutputValueClass(VectorWritable.class);
				job.setOutputKeyClass(IntPairWritable.class);
				job.setOutputValueClass(VectorWritable.class);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
	
				FileInputFormat.addInputPath(job, smoothDir);
				FileOutputFormat.setOutputPath(job, outputDir);
				job.waitForCompletion(true);
			}
			
		} 
		catch (OptionException e) 
		{
			CommandLineUtil.printHelp(group);
		}

		return 0;

	}
}

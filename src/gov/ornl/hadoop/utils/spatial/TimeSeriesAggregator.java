package gov.ornl.hadoop.utils.spatial;


import java.io.IOException;
import java.net.URI;
import java.util.Properties;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.base.Preconditions;

/**
 * Runs a time series aggregation tool on time series data.
 * 
 * Usage:\                                                                          
 * [--input \<input\> --output \<output\> --props /<properties/>] <br> 
 * Options
 *   --input (-i) input             Path to job input directory<br>                   
 *   --output (-o) output           The directory pathname for output<br>             
 *   --props (-p) properties        Properties files		<br>                             
 * 
 * @author chandola
 */

public class TimeSeriesAggregator extends AbstractJob
{
	/**
	 * Runs the TimeSeriesAggregator tool.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new TimeSeriesAggregator(), args);
	}

	public static class Map extends Mapper<IntPairWritable, VectorWritable, IntPairWritable, VectorWritable> 
	{
		Properties props;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			//load properties
			URI[] localFiles = DistributedCache.getCacheFiles(conf);
			Preconditions.checkArgument(localFiles != null && localFiles.length >= 1, 
					"missing paths from the DistributedCache");
			props = Utilities.loadProperties(localFiles[0].toString(),conf);
		}
		
		public void map(IntPairWritable key, VectorWritable value, Context context) throws IOException, InterruptedException {
			DenseVector vector = (DenseVector) value.get();
			int cycle = Integer.parseInt(props.getProperty("cycle"));
			DenseVector sVector = new DenseVector(((int) Math.ceil((vector.size()-2)/cycle)) + 2);
			
			sVector.set(0, vector.get(0));
			sVector.set(1,vector.get(1));
			for(int i = 0; i < sVector.size()-2;i++)
			{
				double s = 0;
				for(int j = i*cycle; j < Math.min((i+1)*cycle,vector.size()-2); j++)
				{
					if(vector.get(j) > 0)
						s++;
				}
				sVector.set(i+2,s);
			}
			context.write(key,new VectorWritable(sVector));
		}
	} 
	
	public static class Reduce extends Reducer<IntPairWritable, VectorWritable, IntPairWritable, VectorWritable>
	{
		public void reduce(IntPairWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException 
		{
		}
	}
	
	public int run(String [] args) throws Exception
	{
		Configuration config = getConf();
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
		ArgumentBuilder abuilder = new ArgumentBuilder();

		GroupBuilder gbuilder = new GroupBuilder();

		Option inputDirOpt = DefaultOptionCreator.inputOption().create();
		Option outputDirOpt = DefaultOptionCreator.outputOption().create();
		Option propsOpt = obuilder.withLongName("props").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("props").create()).withDescription(
						"Location of properties file.").withShortName("p").create();
		Option helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h")
				.create();
		
		Group group = gbuilder.withName("Options").
				withOption(inputDirOpt).
				withOption(outputDirOpt).
				withOption(propsOpt).
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
					!(cmdLine.hasOption(propsOpt)))					
			{
				CommandLineUtil.printHelp(group);
				return -1;
			}
			Path propsFile = new Path((String) cmdLine.getValue(propsOpt));			
			DistributedCache.addCacheFile(propsFile.toUri(), config);
			
			Path inputDir = new Path((String) cmdLine.getValue(inputDirOpt));			
			Path outputDir = new Path((String) cmdLine.getValue(outputDirOpt));
			FileSystem fs = FileSystem.get(config);
			if(fs.exists(outputDir))
				fs.delete(outputDir,true);
			
			Job job = new Job(config, TimeSeriesAggregator.class.getName());
			job.setJarByClass(TimeSeriesAggregator.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(IntPairWritable.class);
			job.setMapOutputValueClass(VectorWritable.class);
			job.setOutputKeyClass(IntPairWritable.class);
			job.setOutputValueClass(VectorWritable.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
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

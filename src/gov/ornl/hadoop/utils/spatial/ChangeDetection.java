package gov.ornl.hadoop.utils.spatial;


import gov.ornl.gpchange.CovFunction;
import gov.ornl.gpchange.GPChange;

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
import org.apache.hadoop.io.LongWritable;
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
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.base.Preconditions;

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

	public static class Map extends Mapper<LongWritable, Text, IntPairWritable, VectorWritable> 
	{
		Properties props;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			URI[] localFiles = DistributedCache.getCacheFiles(conf);
			Preconditions.checkArgument(localFiles != null && localFiles.length >= 1, 
					"missing paths from the DistributedCache");
			props = Utilities.loadProperties(localFiles[0].toString(),conf);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String delim = config.get("delim");
			int numCols = config.getInt("numCols",0);
			
			float ltX = Float.parseFloat(this.props.getProperty("ltX"));
			float ltY = Float.parseFloat(this.props.getProperty("ltY"));
			float rbX = Float.parseFloat(this.props.getProperty("rbX"));
			float rbY = Float.parseFloat(this.props.getProperty("rbY"));
			int nX = Integer.parseInt(this.props.getProperty("nX"));
			int nY = Integer.parseInt(this.props.getProperty("nY"));
			String norm = this.props.getProperty("norm");
			
			float xBinWidth = Math.abs(ltX - rbX)/nX;
			float yBinWidth = Math.abs(ltY - rbY)/nY;
			String [] tokens = value.toString().trim().split(delim);
			if(tokens.length == numCols)
			{
				float x = Float.parseFloat(tokens[0]);
				float y = Float.parseFloat(tokens[1]);
				if(Utilities.isSpatialValid(x,y,ltX,ltY,rbX,rbY))
				{
					int xInd = (int) Math.floor(Math.abs(x - ltX)/xBinWidth);
					int yInd = (int) Math.floor(Math.abs(y - ltY)/yBinWidth);
					IntPairWritable pair = new IntPairWritable(xInd,yInd);
					DenseVector vector = new DenseVector(numCols - 2);
					for(int i = 2; i < numCols; i++)
						vector.set(i-2, Double.parseDouble(tokens[i]));
					//normalize this vector
					DenseVector sVector;
					if(norm == null || norm.compareToIgnoreCase("identity") == 0)
						sVector = vector;
					else if(norm.compareToIgnoreCase("center") == 0)
						sVector = (DenseVector) Utilities.center(vector,0);
					else if(norm.compareToIgnoreCase("zscore") == 0)
						sVector = (DenseVector) Utilities.standardize(vector,0);
					else
						sVector = vector;
					//detect changes
					context.write(pair,new VectorWritable(sVector));
				}
			}
		}
	} 
	
	public static class Reduce extends Reducer<IntPairWritable, VectorWritable, IntPairWritable, VectorWritable>
	{
		Properties props;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			URI[] localFiles = DistributedCache.getCacheFiles(conf);
			Preconditions.checkArgument(localFiles != null && localFiles.length >= 1, 
					"missing paths from the DistributedCache");
			props = Utilities.loadProperties(localFiles[0].toString(),conf);
		}
		
		public void reduce(IntPairWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException 
		{
			java.util.Vector<Vector> data = new java.util.Vector<Vector> ();
			for(VectorWritable v: values)
			{
				data.add(v.get());
			}
			Vector loghyper = ChangeDetection.train(data,props);
			
			if(loghyper != null)
				context.write(key,new VectorWritable(loghyper));
			else
				throw new IllegalArgumentException("Missing Configuation Parameters");
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
				withOption(numColsOpt).
				withOption(outputDirOpt).
				withOption(propsOpt).
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
			
			FileSystem fs = FileSystem.get(config);
			if(fs.exists(outputDir))
				fs.delete(outputDir,true);
				
		
			Job job = new Job(config, "GPTrain");
			job.setJarByClass(ChangeDetection.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setMapOutputKeyClass(IntPairWritable.class);
			job.setMapOutputValueClass(VectorWritable.class);
			job.setOutputKeyClass(IntPairWritable.class);
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
	
	/**
	 * Run training algorithm on data.
	 * 
	 * @param data
	 * @param props
	 * @return Vector of log of hyperparameters
	 */
	public static Vector train(
			java.util.Vector<Vector> data, Properties props) throws IllegalArgumentException
	{
		double [] logHypers = ChangeDetection.getLogHypers(props);
		if(logHypers == null)
		{
			throw new IllegalArgumentException("Could not read hyperparameters");
		}
		if(props.getProperty("cycle") == null || 
				props.getProperty("trainLength") == null || 
				props.getProperty("runLength") == null || 
				props.getProperty("covFunction") == null)
		{
			throw new IllegalArgumentException("Missing parameters.");
		}
		int cycle = Integer.parseInt(props.getProperty("cycle"));
		String funcName = props.getProperty("covFunction");
		CovFunction cse;
		try
		{			
			cse = (CovFunction) Class.forName(funcName).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e)
		{
			throw new IllegalArgumentException("Could not construct covariance function.");
		}
		if(cse.retNumParams() != logHypers.length)
			return null;
		cse.setLogHypers(logHypers);
		cse.setNumParams(logHypers.length);
        
        GPChange gpc = new GPChange(cse);
        int trainLength = Integer.parseInt(props.getProperty("trainLength"));
    	int runLength = Integer.parseInt(props.getProperty("runLength"));
		double[][] trainData = new double[trainLength][data.size()];
		for(int i = 0 ; i < data.size(); i++)
			for(int j = 0; j < trainLength; j++)
				trainData[j][i] = data.get(i).get(j);
		gpc.train(trainData, runLength, 1, 1, 1, cycle);
		DenseVector vLogHypers = new DenseVector(logHypers.length);
		logHypers = gpc.getCovFunc().getLogHypers();
    	for(int i = 0; i < logHypers.length; i++)
    	{
    		vLogHypers.set(i, logHypers[i]);
    	}
        return vLogHypers;
	}

	/**
	 * Loads the hyper-parameters from the properties. No additional check about 
	 * sanity of hyper-parameters is performed. It is assumed that the log of hyper-parameters
	 * are provided in the order expected by the covariance function. The names of the
	 * hyper-parameters are assumed to be of the format param1, param2, and so on. The 
	 * number of hyper-parameters are assumed to be in the property "nHP".
	 * 
	 * @param props Properties object containing hyper-parameters
	 * @return Double array of log of hyper-parameters
	 */
	public static double[] getLogHypers(Properties props) 
	{
		
		if(props.getProperty("nHP") != null)
		{
			int num = Integer.parseInt(props.getProperty("nHP"));
			double[] params = new double[num];
			for(int i = 1; i <= num; i++)
			{
				if(props.getProperty("param"+i) != null)
					params[i-1] = Double.parseDouble(props.getProperty("param"+i));
			}
			
			return params;
		}
		return null;
	}
}

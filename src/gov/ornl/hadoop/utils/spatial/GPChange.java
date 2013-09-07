package gov.ornl.hadoop.utils.spatial;


import gov.ornl.gpchange.CovSEEPNoiseiso;
import gov.ornl.gpchange.GPMonitor;
import gov.ornl.hadoop.utils.IntPair;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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
 * Runs a time series change detection tool on spatial data.
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

public class GPChange extends AbstractJob
{

	
	/**
	 * Runs the ChangeDetection tool.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new GPChange(), args);
	}

	public static class Map extends Mapper<LongWritable, Text, IntPairWritable, VectorWritable> 
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
				if(GPChange.isSpatialValid(x,y,ltX,ltY,rbX,rbY))
				{
					int xInd = (int) Math.floor(Math.abs(x - ltX)/xBinWidth);
					int yInd = (int) Math.floor(Math.abs(y - ltY)/yBinWidth);
					IntPairWritable pair = new IntPairWritable(xInd,yInd);
					DenseVector vector = new DenseVector(numCols);
					for(int i = 0; i < numCols; i++)
						vector.set(i, Double.parseDouble(tokens[i]));
					//normalize this vector
					DenseVector sVector;
					if(norm == null || norm.compareToIgnoreCase("identity") == 0)
						sVector = vector;
					else if(norm.compareToIgnoreCase("center") == 0)
						sVector = (DenseVector) Utilities.center(vector,2);
					else if(norm.compareToIgnoreCase("zscore") == 0)
						sVector = (DenseVector) Utilities.standardize(vector,2);
					else
						sVector = vector;
					
					context.write(pair,new VectorWritable(sVector));
				}
			}
		}
	} 
	
	public static class Reduce extends Reducer<IntPairWritable, VectorWritable, IntPairWritable, VectorWritable>
	{
		Properties props;
		HashMap<IntPair, Vector> logHyperVector = null;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			URI[] localFiles = DistributedCache.getCacheFiles(conf);
			Preconditions.checkArgument(localFiles != null && localFiles.length >= 1, 
					"missing paths from the DistributedCache");
			props = Utilities.loadProperties(localFiles[0].toString(),conf);
			//load model hyperparameters
			if(localFiles.length > 1)
				logHyperVector = GPChange.loadLogHypers(localFiles[1].toString(), conf);

		}
		
		public void reduce(IntPairWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException 
		{
			java.util.Vector<Vector> data = new java.util.Vector<Vector> ();
			for(VectorWritable v: values)
			{
				data.add(v.get());
			}
			java.util.Vector<Vector> changes = GPChange.detectChanges(key,data,props,logHyperVector,2);
			for(Vector v: changes)
			{
				context.write(key,new VectorWritable(v));
			}
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
		Option modelFileOpt = obuilder.withLongName("modelFile").withRequired(false).withArgument(
				abuilder.withMinimum(1).withMaximum(1).withName("modelFile").create()).withDescription(
						"Location of file with hyperparameters.").withShortName("mf").create();
				
		Option helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h")
				.create();
		
		Group group = gbuilder.withName("Options").
				withOption(inputDirOpt).
				withOption(numColsOpt).
				withOption(outputDirOpt).
				withOption(propsOpt).
				withOption(delimOpt).
				withOption(modelFileOpt).
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
			DistributedCache.addCacheFile(propsFile.toUri(), config);
			if(cmdLine.hasOption(modelFileOpt))
			{
				Path modelFile = new Path((String) cmdLine.getValue(modelFileOpt));
				DistributedCache.addCacheFile(modelFile.toUri(), config);
			}
			
			Path inputDir = new Path((String) cmdLine.getValue(inputDirOpt));			
			Path outputDir = new Path((String) cmdLine.getValue(outputDirOpt));
			FileSystem fs = FileSystem.get(config);
			if(fs.exists(outputDir))
				fs.delete(outputDir,true);
			
			Job job = new Job(config, "ChangeDetection");
			job.setJarByClass(GPChange.class);

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
	 * Learn GPChange hyper-parameters from data.
	 * 
	 * @param key
	 * @param data
	 * @param props
	 * @param logHyperVector
	 * @param startIndex
	 * @return Vector of hyper parameters
	 */
	public static java.util.Vector<Vector> detectChanges(
			IntPairWritable key, java.util.Vector<Vector> data, Properties props, HashMap<IntPair, Vector> logHyperVector, int startIndex) 
	{
		double [] logHypers = null;
		
		if(logHyperVector != null)
		{
			IntPair p = new IntPair(key.getFirst(),key.getSecond());
			Vector vec = logHyperVector.get(p);
			if(vec != null)
			{
				logHypers = new double[vec.size()];
				for(int i = 0; i < vec.size(); i++)
					logHypers[i] = vec.get(i);					
			}
		}
		if(logHypers == null)
			logHypers = GPChange.getLogHypers(props);
		int cycle = Integer.parseInt(props.getProperty("cycle"));
        CovSEEPNoiseiso cse = new CovSEEPNoiseiso(logHypers,logHypers.length);
        gov.ornl.gpchange.GPChange gpc = new gov.ornl.gpchange.GPChange(cse);
        int length = data.get(0).size() - startIndex;
		double[][] testData = new double[length][data.size()];
		for(int i = 0 ; i < data.size(); i++)
			for(int j = 0; j < length; j++)
				testData[j][i] = data.get(i).get(j+startIndex);
        double alpha = 0.01;
        if(props.getProperty("alpha") != null)
        	alpha = Double.parseDouble(props.getProperty("alpha"));
        //monitor from the first observation
        GPMonitor gpm = gpc.monitor(testData, data.size(), cycle, alpha, null, 1);
        double[][] z = gpm.getZ();
        java.util.Vector<Vector> vector = new java.util.Vector<Vector>(data.size());
        for(int i = 0; i < data.size(); i++)
        {
        	DenseVector dv = new DenseVector(length+startIndex);
        	for(int j = 0; j < startIndex; j++)
        		dv.set(j,data.get(i).get(j));
        	for(int j = startIndex; j < length+startIndex; j++)
        		dv.set(j,z[j-startIndex][i]);
        	vector.add(dv);
        }

        return vector;
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

	/**
	 * Checks if a given point is inside the spatial bounding box.
	 * 
	 * @param x - x coordinate of query point
	 * @param y - y coordinate of query point
	 * @param ltX - x coordinate of top left corner of the bounding box
	 * @param ltY - y coordinate of top left corner of the bounding box
	 * @param rbX - x coordinate of bottom right corner of the bounding box
	 * @param rbY - y coordinate of bottom right corner of the bounding box
	 * @return true if point is within the box, false otherwise
	 */
	public static boolean isSpatialValid(float x, float y, float ltX, float ltY, float rbX, float rbY) 
	{
		boolean xFlag,yFlag;
		if(ltX < rbX)
			xFlag = ltX <= x && rbX >= x;
		else
			xFlag = rbX <= x && ltX >= x;
		if(ltY < rbY)
			yFlag = ltY <= y && rbY >= y;
		else
			yFlag = rbY <= y && ltY >= y;
		return xFlag && yFlag;
	}
	
	/**
	 * Load loghypers file
	 * 
	 * @param logHyperFile
	 * @param config
	 * @return {@ HashMap} object mapping (xbin,ybin) to the loghypers
	 */
	public static HashMap<IntPair, Vector> loadLogHypers(String logHyperFile, Configuration config)
	{
		try
		{
			Path[] paths;
			FileSystem fs = FileSystem.get(config);
			if(fs.isFile(new Path(logHyperFile)))
			{
				paths = new Path[]{new Path(logHyperFile)};
			}
			else
			{
				FileStatus[] files = fs.globStatus(new Path(logHyperFile+"/part-*"));
				paths = new Path[files.length];
				for(int i = 0; i < files.length; i++)
				{
					paths[i] = files[i].getPath();
				}
			}
			HashMap<IntPair, Vector> logHyperVector = new HashMap<IntPair, Vector>(); 
			for(Path p: paths)
			{
				SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), p, config);
				IntPairWritable key = (IntPairWritable) reader.getKeyClass().newInstance();
				VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
				while(reader.next(key,value))
					logHyperVector.put(new IntPair(key.getFirst(),key.getSecond()),value.get());
				reader.close();
			}
			return logHyperVector;
		} catch (Exception e)
		{			
			e.printStackTrace();
		}
		return null;
	}
}

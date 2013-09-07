package gov.ornl.hadoop.utils.spatial;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

public class Utilities 
{

	/**
	 * Standardize values of a vector starting with index s
	 * 
	 * @param vector - vector to be standardized
	 * @param s - starting index (starts with 0)
	 */
	public static Vector standardize(Vector vector, int s) 
	{
		double mean = 0, std = 0;
		for(int i = s; i < vector.size(); i++)
			mean += vector.get(i);
		
		mean /= (vector.size() - s);
		for(int i = s; i < vector.size(); i++)
			std += Math.pow(vector.get(i)-mean,2);
		std /= (vector.size() - s - 1);
		std = Math.sqrt(std);
		if(std == 0) std = 1;
		Vector sVector = vector.clone();
		for(int i = s; i < vector.size(); i++)
			sVector.set(i, (vector.get(i) - mean)/std);
		return sVector;
	}
	
	/**
	 * Center values of a vector to zero mean
	 * 
	 * @param vector - vector to be centered
	 * @param s - starting index (starts with 0)
	 */
	public static Vector center(Vector vector, int s) 
	{
		double mean = 0, std = 0;
		for(int i = s; i < vector.size(); i++)
			mean += vector.get(i);
		
		mean /= (vector.size() - s);
		for(int i = s; i < vector.size(); i++)
			std += Math.pow(vector.get(i)-mean,2);
		std /= (vector.size() - s);
		std = Math.sqrt(std);
		if(std == 0) std = 1;
		Vector sVector = vector.clone();
		for(int i = s; i < vector.size(); i++)
			sVector.set(i, (vector.get(i) - mean)/std);
		return sVector;
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
	 * Load properties files
	 * 
	 * @param propsFile
	 * @param config
	 * @return {@link Properties} object
	 */
	public static Properties loadProperties(String propsFile, Configuration config)
	{
		try
		{
			FileSystem fs = FileSystem.get(config);	
			BufferedInputStream in = new BufferedInputStream(fs.open(new Path(propsFile)));
			Properties props = new Properties();
			props.load(in);
			return props;
		}
		catch(IOException e)
		{
			System.err.println("IO Error");
			e.printStackTrace();
			return null;
		}
	}
	
}

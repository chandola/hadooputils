package gov.ornl.hadoop.utils.spatial;

import gpchange.CovFunction;
import gpchange.CovSEEPNoiseiso;

public class Tester
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		String funcName = "gpchange.CovSEEPNoiseiso";
		CovFunction cse;
		try
		{
			Class c = Class.forName(funcName);
			System.out.println(c.getName());
			cse = (CovFunction) Class.forName(funcName).newInstance();
			System.out.println(cse.getNumParams());
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e)
		{
			throw new IllegalArgumentException("Could not construct covariance function.");
		}

	}

}

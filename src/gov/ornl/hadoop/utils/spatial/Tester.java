package gov.ornl.hadoop.utils.spatial;

import gov.ornl.gpchange.CovFunction;

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
			cse = (CovFunction) Class.forName(funcName).newInstance();
			System.out.println(cse.getNumParams());
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e)
		{
			throw new IllegalArgumentException("Could not construct covariance function.");
		}

	}

}

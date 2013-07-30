/**
 * 
 */
package gov.ornl.hadoop.utils;

import java.io.Serializable;

/**
 * @author chandola
 *
 */
public final class DoublePair implements Serializable
{
	private static final long serialVersionUID = 1L;
	private double one;
	private double two;
	
	public DoublePair(double one, double two)
	{
		this.one = one;
		this.two = two;
	}
	
	public double getOne()
	{
		return this.one;
	}
	
	public double getTwo()
	{
		return this.two;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if(other instanceof DoublePair)
			if(this.one == ((DoublePair) other).getOne() && this.two == ((DoublePair) other).getTwo())
				return true;
		return false;
	}
	
	@Override
	public String toString()
	{
		return "<"+this.one+","+this.two+">";
	}
	
	@Override
	public int hashCode()
	{
		return (new String(this.one+" "+this.two)).hashCode();
	}
}

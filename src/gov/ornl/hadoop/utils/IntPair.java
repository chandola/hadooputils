/**
 * 
 */
package gov.ornl.hadoop.utils;

import java.io.Serializable;

/**
 * @author chandola
 *
 */
public final class IntPair implements Serializable
{
	private static final long serialVersionUID = 1L;
	private int one;
	private int two;
	
	public IntPair(int one, int two)
	{
		this.one = one;
		this.two = two;
	}
	
	public int getOne()
	{
		return this.one;
	}
	
	public int getTwo()
	{
		return this.two;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if(other instanceof IntPair)
			if(this.one == ((IntPair) other).getOne() && this.two == ((IntPair) other).getTwo())
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

package com.proj.minMaxTaxiOutFromDest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MinMaxTaxiOutTuple implements WritableComparable{
	
	long minTaxiOut = Long.MIN_VALUE;
	long maxTaxiOut = Long.MAX_VALUE;

	public long getMinTaxiOut() {
		return minTaxiOut;
	}

	public void setMinTaxiOut(long minTaxiOut) {
		this.minTaxiOut = minTaxiOut;
	}

	public long getMaxTaxiOut() {
		return maxTaxiOut;
	}

	public void setMaxTaxiOut(long maxTaxiOut) {
		this.maxTaxiOut = maxTaxiOut;
	}


	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		minTaxiOut = in.readLong();
		maxTaxiOut = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(minTaxiOut);
		out.writeLong(maxTaxiOut);
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "Min TaxiOut: " + minTaxiOut + " Max TaxiOut: " + maxTaxiOut ;
	}

}

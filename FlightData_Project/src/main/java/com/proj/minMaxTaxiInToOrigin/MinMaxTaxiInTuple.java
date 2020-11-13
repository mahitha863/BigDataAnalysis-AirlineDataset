package com.proj.minMaxTaxiInToOrigin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MinMaxTaxiInTuple implements WritableComparable{
	
	long minTaxiIn = Long.MIN_VALUE;
	long maxTaxiIn = Long.MAX_VALUE;

	public long getMinTaxiIn() {
		return minTaxiIn;
	}

	public void setMinTaxiIn(long minTaxiIn) {
		this.minTaxiIn = minTaxiIn;
	}

	public long getMaxTaxiIn() {
		return maxTaxiIn;
	}

	public void setMaxTaxiIn(long maxTaxiIn) {
		this.maxTaxiIn = maxTaxiIn;
	}


	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		minTaxiIn = in.readLong();
		maxTaxiIn = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(minTaxiIn);
		out.writeLong(maxTaxiIn);
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "Min TaxiIn: " + minTaxiIn + " Max TaxiIn: " + maxTaxiIn ;
	}

}

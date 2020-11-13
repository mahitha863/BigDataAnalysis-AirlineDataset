package com.proj.averageTaxiOutTimeFromDest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAverageWritable implements Writable {
	
	long count = 0;
	double average = 0.0;
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		count =  in.readLong();
		average = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(count);
		out.writeDouble(average);
	}
	
	public String toString() {
		String avg = (String) String.format("%.2f", average);
		return "Average TaxiOut time = " + avg;
	}

}

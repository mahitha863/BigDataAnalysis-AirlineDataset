package com.proj.secsortTripsByDistance;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class NaturalKeyGroupComparator extends WritableComparator{
	
	public NaturalKeyGroupComparator() {
		super(CompositeKeyWritable.class, true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKeyWritable ck1 = (CompositeKeyWritable)a;
		CompositeKeyWritable ck2  =  (CompositeKeyWritable)b;
		
		int result = ck1.getDistance().compareTo(ck2.getDistance());
		return  result;
	}

}

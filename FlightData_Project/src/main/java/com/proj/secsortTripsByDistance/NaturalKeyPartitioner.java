package com.proj.secsortTripsByDistance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKeyWritable,IntWritable>{

	@Override
	public int getPartition(CompositeKeyWritable key, IntWritable val, int numPartitions) {
		// TODO Auto-generated method stub
		return key.getDistance().hashCode() % numPartitions;
	}

}

package com.proj.numOfFlightTravels;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalCountReducer extends Reducer<NullWritable,IntWritable,NullWritable,IntWritable> {
	IntWritable count = new IntWritable();
	int sum = 0;
	@Override
	public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for(IntWritable val : values) {
			sum += val.get();
		}
		count.set(sum);
		context.write(key, count);
	}

}

package com.proj.secsortTripsByDistance;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TripsByDistanceReducer extends Reducer<CompositeKeyWritable, IntWritable, Text, IntWritable> {

	Text k = new Text();
	protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values,
			Reducer<CompositeKeyWritable, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		// TODO Auto-generated method stub
		for (IntWritable val : values) {
			sum += val.get();
		}
		IntWritable count = new IntWritable(sum);
		k.set(key.getRoute() + " " + key.getDistance());
		context.write(k, count);
	}

}

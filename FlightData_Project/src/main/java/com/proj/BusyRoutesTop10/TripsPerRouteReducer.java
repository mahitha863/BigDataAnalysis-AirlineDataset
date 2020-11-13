package com.proj.BusyRoutesTop10;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TripsPerRouteReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	IntWritable count = new IntWritable();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable val : values) {
			sum += val.get();
		}
		count.set(sum);
		context.write(key, count);
	}

}

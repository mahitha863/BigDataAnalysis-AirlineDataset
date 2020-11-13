package com.proj.numFlightsCancelledPerReason;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCancelledReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	LongWritable count = new LongWritable();
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable val : values) {
			sum += val.get();
		}
		count.set(sum);
		context.write(key, count);
	}

}

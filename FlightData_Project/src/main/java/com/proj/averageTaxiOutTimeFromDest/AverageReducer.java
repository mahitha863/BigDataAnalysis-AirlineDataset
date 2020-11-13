package com.proj.averageTaxiOutTimeFromDest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AverageReducer extends Reducer<Text, CountAverageWritable, Text, CountAverageWritable>{
	
	CountAverageWritable result = new CountAverageWritable();
	
	@Override
	public void reduce(Text key, Iterable<CountAverageWritable> values, Context context) throws IOException, InterruptedException {
		
		double sum = 0.0;
		long count = 0;
		
		for(CountAverageWritable  val : values) {
			sum += val.getCount() * val.getAverage();
			count += val.getCount();
		}
		
		result.setCount(count);
		result.setAverage(sum/count);
		
		context.write(key, result);
		
	}

}

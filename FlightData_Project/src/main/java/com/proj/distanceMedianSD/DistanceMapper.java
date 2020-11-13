package com.proj.distanceMedianSD;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistanceMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
	
	private Text year = new Text();
	private LongWritable dist = new LongWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
	        String[] tokens= line.split(",");
	        
	        year.set(tokens[0]);
	        if(tokens[18].equalsIgnoreCase("NA")) {
	        	return;
	        }
	        try {
	        	dist.set(Long.parseLong(tokens[18]));
	        } catch(NumberFormatException e) {
	        	return;
	        }
	        
	        context.write(year, dist);
		}
	}

}

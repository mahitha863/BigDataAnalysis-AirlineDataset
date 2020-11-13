package com.proj.BusyRoutesTop10;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TripsPerRouteMapper extends Mapper<LongWritable,Text,Text,IntWritable>  {
	
	Text route = new Text();
	IntWritable one = new IntWritable(1);
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			route.set(tokens[16] + " - " + tokens[17]);
			context.write(route, one);
		}
	 }

}

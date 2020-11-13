package com.proj.secsortTripsByDistance;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TripsByDistanceMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable>  {

	IntWritable one = new IntWritable(1);
	String route = "";
	Long distance;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.get() > 0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			route = tokens[16] + " - " + tokens[17];
			try {
				distance = Long.parseLong(tokens[18]);
			}catch(NumberFormatException e) {
				return;
			}
			CompositeKeyWritable ck = new CompositeKeyWritable(route, distance);
			context.write(ck, one);
		}
	}
}

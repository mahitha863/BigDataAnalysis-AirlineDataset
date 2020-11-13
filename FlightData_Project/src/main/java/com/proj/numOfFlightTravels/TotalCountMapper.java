package com.proj.numOfFlightTravels;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalCountMapper extends Mapper<LongWritable,Text,NullWritable,IntWritable>  {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.get()>0) {
			context.write(NullWritable.get(), new IntWritable(1));
		}
	}
	

}

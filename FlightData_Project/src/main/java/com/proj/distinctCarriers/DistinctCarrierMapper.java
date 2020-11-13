package com.proj.distinctCarriers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctCarrierMapper extends Mapper<LongWritable,Text,Text,NullWritable>  {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			String carrier = tokens[8];
			
			context.write(new Text(carrier), NullWritable.get());
		}
	}

}

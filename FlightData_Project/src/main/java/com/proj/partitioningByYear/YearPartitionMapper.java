package com.proj.partitioningByYear;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearPartitionMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	private  IntWritable yrMapKey = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			
			int year = Integer.parseInt(tokens[0]);
			String row = tokens[0] + " " + tokens[8] + " " + tokens[9] + " " + 
			               tokens[16] + " " + tokens[17] ;
			
			yrMapKey.set(year);
			
			context.write(yrMapKey, new Text(row));
		}
	}

}

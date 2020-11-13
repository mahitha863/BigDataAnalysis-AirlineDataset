package com.proj.busyWeekDay;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusyWeekDayMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			String weekDay = tokens[3];
			context.write(new Text(weekDay), new IntWritable(1));
		}
	}

}

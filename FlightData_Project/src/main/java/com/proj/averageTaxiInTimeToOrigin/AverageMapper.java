package com.proj.averageTaxiInTimeToOrigin;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AverageMapper extends Mapper<LongWritable, Text, Text, CountAverageWritable> {
	
	Text origin = new Text();
	CountAverageWritable countAvg = new CountAverageWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			
			long taxiIn = 0;
			try{
				if(tokens[19]=="NA")
					taxiIn = 0;
				else
					taxiIn = Long.parseLong(tokens[19]);
			}catch(NumberFormatException e) {
				return;
			}
			countAvg.setAverage(taxiIn);
			countAvg.setCount(1);
			
			origin.set(tokens[16]);
			context.write(origin, countAvg);
		}
	}

}
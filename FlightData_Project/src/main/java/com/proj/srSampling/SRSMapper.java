package com.proj.srSampling;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SRSMapper extends Mapper<LongWritable,Text,NullWritable,Text>  {
	
	private Random rands = new Random();
	private Double percentage;
	
	protected void setup(Context context) {
		String strPercentage = context.getConfiguration().get("filter.percentage");
		percentage = Double.parseDouble(strPercentage) / 100.0;
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(rands.nextDouble() < percentage) {
			context.write(NullWritable.get(), value);
		}
	}

}

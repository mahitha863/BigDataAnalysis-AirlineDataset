package com.proj.minMaxTaxiInToOrigin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxTaxiInMapper extends Mapper<LongWritable, Text, Text, MinMaxTaxiInTuple>  {
	
	Text origin = new Text();
	MinMaxTaxiInTuple tuple = new MinMaxTaxiInTuple();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			long taxiIn;
			try {
				if(tokens[19]=="NA") return;
				else {
					taxiIn =  Long.parseLong(tokens[19]);
				}
			}catch(NumberFormatException e) {
				return;
			}
			tuple.setMaxTaxiIn(taxiIn);
			tuple.setMinTaxiIn(taxiIn);
			
			origin.set(tokens[16]);
			context.write(origin, tuple);
		}
	}

}

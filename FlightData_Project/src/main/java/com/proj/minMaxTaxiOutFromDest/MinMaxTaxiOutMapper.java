package com.proj.minMaxTaxiOutFromDest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxTaxiOutMapper extends Mapper<LongWritable, Text, Text, MinMaxTaxiOutTuple>  {
	
	Text dest = new Text();
	MinMaxTaxiOutTuple tuple = new MinMaxTaxiOutTuple();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			long taxiOut;
			try {
				if(tokens[20]=="NA") return;
				else {
					taxiOut =  Long.parseLong(tokens[20]);
				}
			}catch(NumberFormatException e) {
				return;
			}
			tuple.setMaxTaxiOut(taxiOut);
			tuple.setMinTaxiOut(taxiOut);
			
			dest.set(tokens[17]);
			context.write(dest, tuple);
		}
	}

}

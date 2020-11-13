package com.proj.flightNumsUnderCarrier;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexByCarrierMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
	        String[] tokens= line.split(",");
	        
	        context.write(new Text(tokens[8]), new Text(tokens[9]));
		}
	}

}

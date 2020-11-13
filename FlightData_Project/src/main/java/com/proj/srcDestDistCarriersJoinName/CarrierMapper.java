package com.proj.srcDestDistCarriersJoinName;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarrierMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text code = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			line = line.replace("\"", "");
			String[] tokens = line.split(",");
			code.set(tokens[0]);
			String outValue= "A"+tokens[1];
			context.write(code, new Text(outValue));
		}
	}

}

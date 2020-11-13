package com.proj.srcDestDistCarriersJoinName;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, Text, Text> {

Text code = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
			String[] tokens = line.split(",");
			code.set(tokens[8]);
			String outValue= "B"+tokens[16]+"-"+tokens[17]+" "+tokens[18];
			context.write(code, new Text(outValue));
		}
	}
	
}

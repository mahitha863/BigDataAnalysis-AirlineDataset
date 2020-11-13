package com.proj.numFlightsCancelledPerReason;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCancelledMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get()>0) {
			String line = value.toString();
	        String[] tokens= line.split(",");
	        
	        String cancelCode = tokens[22];
	        String code = "";
	        if(cancelCode.isEmpty() || cancelCode == null || tokens[21]=="0")
	        	code = "Not Cancelled";
	        else if(cancelCode.equals("A"))
	        	code = "Carrier";
	        else if(cancelCode.equals("B"))
	        	code = "Weather";
	        else if(cancelCode.equals("C"))
	        	code = "NAS";
	        else if(cancelCode.equals("D"))
	        	code = "Security";
	        else
	        	code = "Not Cancelled";
	        context.write(new Text(code), new LongWritable(1));
		}
	}
			
		

}

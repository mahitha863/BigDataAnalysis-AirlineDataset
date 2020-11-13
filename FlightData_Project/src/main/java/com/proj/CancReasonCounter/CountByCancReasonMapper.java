package com.proj.CancReasonCounter;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountByCancReasonMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>{
	
	public static final String CANCEL_REASON = "Reason";
	public static final String UNKNOWN_COUNTER = "Unknown";
	public static final String NULL_OR_EMPTY_COUNTER = "Not Cancelled";
	
	private String[] reasonsArr = new String[] {"A", "B", "C", "D"};
	
	private HashSet<String> reasons = new HashSet<String>(Arrays.asList(reasonsArr));
	
	public void map(LongWritable key, Text value, Context context) {
		if(key.get()>0) {
			String line = value.toString();
	        String[] tokens= line.split(",");
	        String cancelCode =  tokens[22];
	        if(cancelCode!=null && !cancelCode.isEmpty() && tokens[21]!="0") {
	        	boolean unknown =  true;
	        	if(reasons.contains(cancelCode)) {
	        		context.getCounter(CANCEL_REASON, getCodeName(cancelCode)).increment(1);;
	        		unknown = false;
	        	}
	        	if(unknown) {
	        		context.getCounter(UNKNOWN_COUNTER, getCodeName(cancelCode)).increment(1);;
	        	}
	        }else {
	        	context.getCounter(NULL_OR_EMPTY_COUNTER, "Not Cancelled");
	        }
		}
	}
	
	public String getCodeName(String code) {
		if(code.equalsIgnoreCase("A")) 
			return "carrier";
		else if(code.equalsIgnoreCase("B"))
			return "weather";
		else if(code.equalsIgnoreCase("C"))
			return "NAS";
		else if(code.equalsIgnoreCase("D"))
			return "Security";
		else
			return "Not Cancelled";
	}

}

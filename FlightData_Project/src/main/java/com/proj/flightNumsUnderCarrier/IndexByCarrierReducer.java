package com.proj.flightNumsUnderCarrier;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexByCarrierReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text result = new Text();
	ArrayList<String> list = new ArrayList<String>(); // To insert only unique flight numbers
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		list.clear();
		StringBuilder sb = new StringBuilder();
		for(Text carrier :  values) {
			if(!list.contains(carrier.toString()))
				list.add(carrier.toString());
		}
		boolean first = true;
		for(String carr : list) {
			if(first)
				first = false;
			else
				sb.append(",");
			sb.append(carr.toString());
		}
		result.set(sb.toString());
		context.write(key, result);
	}

}

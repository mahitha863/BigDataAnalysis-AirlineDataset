package com.proj.minMaxTaxiInToOrigin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxTaxiInReducer extends Reducer<Text,MinMaxTaxiInTuple,Text,MinMaxTaxiInTuple> {
	
	@Override
	public void reduce(Text key, Iterable<MinMaxTaxiInTuple> values, Context context) throws IOException, InterruptedException {
		long minTaxiIn = Long.MAX_VALUE;
		long maxTaxiIn = Long.MIN_VALUE;
		
		for(MinMaxTaxiInTuple val : values) {
			if(val.getMinTaxiIn() < minTaxiIn)
				minTaxiIn = val.getMinTaxiIn();
			if(val.getMaxTaxiIn() > maxTaxiIn)
				maxTaxiIn = val.getMaxTaxiIn();
		}
		MinMaxTaxiInTuple tup = new MinMaxTaxiInTuple();
		tup.setMaxTaxiIn(maxTaxiIn);
		tup.setMinTaxiIn(minTaxiIn);
		context.write(key, tup);
	}

}

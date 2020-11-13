package com.proj.minMaxTaxiOutFromDest;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxTaxiOutReducer extends Reducer<Text,MinMaxTaxiOutTuple,Text,MinMaxTaxiOutTuple> {
	
	@Override
	public void reduce(Text key, Iterable<MinMaxTaxiOutTuple> values, Context context) throws IOException, InterruptedException {
		long minTaxiOut = Long.MAX_VALUE;
		long maxTaxiOut = Long.MIN_VALUE;
		
		for(MinMaxTaxiOutTuple val : values) {
			if(val.getMinTaxiOut() < minTaxiOut)
				minTaxiOut = val.getMinTaxiOut();
			if(val.getMaxTaxiOut() > maxTaxiOut)
				maxTaxiOut = val.getMaxTaxiOut();
		}
		MinMaxTaxiOutTuple tup = new MinMaxTaxiOutTuple();
		tup.setMaxTaxiOut(maxTaxiOut);
		tup.setMinTaxiOut(minTaxiOut);
		context.write(key, tup);
	}

}

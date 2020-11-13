package com.proj.distanceMedianSD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistanceReducer extends Reducer<Text, LongWritable,Text,MedianStdDevTuple> {
	
	MedianStdDevTuple tuple = new MedianStdDevTuple();
	ArrayList<Float> list = new ArrayList<Float>();
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		 float sum =0;
	     float count=0;
	     list.clear();
	     tuple.setMedianDist(0);
	     
	     for(LongWritable val : values) {
	    	 list.add((float)val.get());
	    	 sum += (float)val.get();
	    	 ++count;
	     }
	     
	     Collections.sort(list);
	     
	     if(count%2==0){
	    	 tuple.setMedianDist((list.get((int)count/2-1) + list.get((int)count/2))/2.0f);
	     }else {
	    	 tuple.setMedianDist(list.get((int)count/2));
	     }
	     
	     float mean = sum/count;
	     float sumOfSquares =0.0f;
	     for(Float f :list){
	            sumOfSquares += (f-mean) * (f-mean);
	     }
	     
	     tuple.setStdDevDist((float)Math.sqrt(sumOfSquares/(count-1)));
	     context.write(key, tuple);
	     
	}

}

package com.proj.busyWeekDay;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusyWeekDayReducer extends Reducer<Text,IntWritable,Text,Text> {
	
	int total_count =0;
    HashMap<String,IntWritable> map;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map = new HashMap<String, IntWritable>();
    }
	
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	int count = 0;
        for(IntWritable val:values){
            count += val.get();
        }
        map.put(key.toString(),new IntWritable(count));
        total_count += count;
	}
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry <String,IntWritable> entry: map.entrySet()){
            int v = entry.getValue().get();
            float percent = (float) v/total_count;
            percent = percent*100;
            context.write(new Text(entry.getKey()), new Text(entry.getValue() + " / " + total_count + " = " +
                    percent+"% Busy"));
        }
    }

}

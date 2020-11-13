package com.proj.busyWeekDay;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusyWeekDayCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum=0;
        for(IntWritable a:values){
            sum+=a.get();
        }

        context.write(key,new IntWritable(sum));
	}

}

package com.proj.top10BusyRoutes;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10BusyRoutesReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

private TreeMap<Long,  String> topTenMap ;
	
	public void setup(Context context) {
		topTenMap = new TreeMap<Long, String>();
	}
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text,LongWritable,Text,LongWritable>.Context context) {
		String route  =  key.toString();
		long tripsCount = 0;
		for(LongWritable val : values) {
			tripsCount = val.get();
		}
		topTenMap.put(tripsCount, route);
		
		if(topTenMap.size() > 10) {
			topTenMap.remove(topTenMap.firstKey());
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		for(Map.Entry<Long, String> m : topTenMap.descendingMap().entrySet()) {
			context.write(new Text(m.getValue()), new LongWritable(m.getKey()));
		}
	}
}

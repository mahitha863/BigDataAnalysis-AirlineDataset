package com.proj.top10BusyRoutes;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10BusyRoutesMapper extends Mapper<LongWritable,Text,Text,LongWritable>  {
	
	private TreeMap<Long,  String> topTenMap ;
	
	public void setup(Context context) {
		topTenMap = new TreeMap<Long, String>();
	}

	public void map(LongWritable key, Text value, Context context) {
		String line = value.toString();
		String[] tokens = line.split("\\t");
		String route = tokens[0];
		Long tripsCount = Long.parseLong(tokens[1]);
		topTenMap.put(tripsCount, route);
		
		if(topTenMap.size() > 10) {
			topTenMap.remove(topTenMap.firstKey());
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Map.Entry<Long, String> m : topTenMap.entrySet()) {
			context.write(new Text(m.getValue()), new LongWritable(m.getKey()));
		}
	}
}

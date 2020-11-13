package com.proj.BusyRoutesTop10;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
    public static void main( String[] args )
    {
    	long start = System.currentTimeMillis();
    	Path out = new Path(args[1]);
    	Configuration conf = new Configuration();
    	try {
			Job job1 = Job.getInstance(conf, "Trips per route");
			job1.setJarByClass(Driver.class);
			
			//set mappers and reducers classes
			job1.setMapperClass(TripsPerRouteMapper.class);
			job1.setReducerClass(TripsPerRouteReducer.class);
			
			//set inputformat and outputformat
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			
			
			//set the output key and value types
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			
			//set the input and output args
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(out,"mrout1"));
			
//			System.exit(job1.waitForCompletion(true)? 0:1);
			if (!job1.waitForCompletion(true)) {
			      System.exit(1);
			    }
			
//			************ second job *************
			Job job2 = Job.getInstance(conf, "Top 10 busy routes");
			job2.setJarByClass(Driver.class);
			
			//set mappers and reducers classes
			job2.setMapperClass(Top10BusyRoutesMapper.class);
			job2.setReducerClass(Top10BusyRoutesReducer.class);
//			job2.setNumReduceTasks(0);
			
			//set inputformat and outputformat
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			
			//set the output key and value types
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(LongWritable.class);
			
			//set the input and output args
			FileInputFormat.addInputPath(job2, new Path(out,"mrout1"));
			FileOutputFormat.setOutputPath(job2, new Path(out,"mrout2"));
			
			
			//System.exit(job2.waitForCompletion(true)? 0:1);
			
			if(job2.waitForCompletion(true)) {
				 long end = System.currentTimeMillis();
			     float timeTaken = (end - start)/1000F;
			     System.out.println("time taken = "+Float.toString(timeTaken));
				 System.exit(0);
			}else {
				System.exit(1);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}

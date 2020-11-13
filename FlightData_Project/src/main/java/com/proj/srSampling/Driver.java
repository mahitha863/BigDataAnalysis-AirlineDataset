package com.proj.srSampling;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
    public static void main( String[] args )
    {
    	Configuration conf = new Configuration();
    	try {
			Job job = Job.getInstance(conf, "SRS sampling");
			job.setJarByClass(Driver.class);
			
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			//set inputformat and outputformat
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			
			//set mappers and reducers classes
			job.setMapperClass(SRSMapper.class);
			job.setNumReduceTasks(0);
			
			job.getConfiguration().set("filter.percentage", "1");
			//set the input and output args
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//deletes if o/p path already exist
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(args[1]),true);
			
			//System.exit(job.waitForCompletion(true)? 0:1);
			long start = System.currentTimeMillis();
			if(job.waitForCompletion(true)) {
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

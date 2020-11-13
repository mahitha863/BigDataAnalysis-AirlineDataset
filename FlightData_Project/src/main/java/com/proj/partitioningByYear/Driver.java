package com.proj.partitioningByYear;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{


        Configuration conf = new Configuration();
        // Create a new Job
        Job job = Job.getInstance(conf,"Partitioning By Year");
        job.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setMapperClass(YearPartitionMapper.class);
        // Set custom partitioner and min last access date
        job.setPartitionerClass(YearPartitioner.class);
        YearPartitioner.setMinLastAccessDate(job, 1987);

        job.setNumReduceTasks(22);

        job.setReducerClass(YearPartitionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        //deletes if o/p path already exist
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);

        // Submit the job, then poll for progress until the job is complete
        //System.exit(job.waitForCompletion(true)?0:1);
        long start = System.currentTimeMillis();
		if(job.waitForCompletion(true)) {
			 long end = System.currentTimeMillis();
		     float timeTaken = (end - start)/1000F;
		     System.out.println("time taken = "+Float.toString(timeTaken));
			 System.exit(0);
		}else {
			System.exit(1);
		}

    }
}

package com.proj.CancReasonCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Counting With Counters");
        job.setJarByClass(Driver.class);
        job.setMapperClass(CountByCancReasonMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Path out = new Path(args[1]);
        FileSystem.get(conf).delete(out, true);
        int code = job.waitForCompletion(true) ? 0 : 1;


        SortedMap<Long, String> tm = new TreeMap<Long,String>(new Comparator<Long>() {
            public int compare(Long s1, Long s2) {
                return s2.compareTo(s1);
            }
        });

        if (code == 0) {
            /*for (Counter counter : job.getCounters().getGroup(CountByCancReasonMapper.CANCEL_REASON)) {
                {
                    tm.put(counter.getValue(),counter.getDisplayName());
                }
            }*/
        	Counter counterA = (Counter) (job.getCounters().findCounter(CountByCancReasonMapper.CANCEL_REASON, "carrier"));
        	tm.put(counterA.getValue(),counterA.getDisplayName());
        	
        	Counter counterB = (Counter) (job.getCounters().findCounter(CountByCancReasonMapper.CANCEL_REASON, "weather"));
        	tm.put(counterB.getValue(),counterB.getDisplayName());
        	
        	Counter counterC = (Counter) (job.getCounters().findCounter(CountByCancReasonMapper.CANCEL_REASON, "NAS"));
        	tm.put(counterC.getValue(),counterC.getDisplayName());
        	
        	Counter counterD = (Counter) (job.getCounters().findCounter(CountByCancReasonMapper.CANCEL_REASON, "Security"));
        	tm.put(counterD.getValue(),counterD.getDisplayName());
        	
        	Counter counterNC = (Counter) (job.getCounters().findCounter(CountByCancReasonMapper.CANCEL_REASON, "Not Cancelled"));
        	tm.put(counterNC.getValue(),counterNC.getDisplayName());
            for(Map.Entry mentry:tm.entrySet()){
                System.out.println("CancelReason : " + mentry.getValue() + " flights_count " + mentry.getKey());
            }

        }
        System.exit(code);
    }

}

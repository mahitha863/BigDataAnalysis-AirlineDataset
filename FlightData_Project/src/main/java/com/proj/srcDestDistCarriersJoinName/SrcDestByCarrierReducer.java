package com.proj.srcDestDistCarriersJoinName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SrcDestByCarrierReducer extends Reducer<Text, Text, Text, Text> {
	
    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    public void setup(Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get("join.type");
    }
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listA.clear();
        listB.clear();
        
        
        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();

            if (tmp.charAt(0) == 'A') {
                listA.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt(0) == 'B') {
                listB.add(new Text(tmp.toString().substring(1)));
            }
        }

        executeJoinLogic(context);
     }
    
    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
        if(joinType.equals("inner")){
            if(!listA.isEmpty() && !listB.isEmpty()){
                for(Text textA:listA){
                    for(Text textB:listB){
                        context.write(textA,textB);
                    }
                }
            }
        }
    }


}

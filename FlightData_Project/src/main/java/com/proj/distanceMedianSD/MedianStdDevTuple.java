package com.proj.distanceMedianSD;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MedianStdDevTuple implements Writable {
	
	private float stdDevDist;
    private float medianDist;
    
    public MedianStdDevTuple() {
    	super();
    }

	public float getStdDevDist() {
		return stdDevDist;
	}

	public void setStdDevDist(float stdDevDist) {
		this.stdDevDist = stdDevDist;
	}

	public float getMedianDist() {
		return medianDist;
	}

	public void setMedianDist(float medianDist) {
		this.medianDist = medianDist;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		stdDevDist=in.readFloat();
        medianDist=in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(stdDevDist);
        out.writeFloat(medianDist);
	}
	
	@Override
    public String toString() {
        return "Standard Deviation : "+ stdDevDist+";  Median_Distance: " + medianDist;
    }

}

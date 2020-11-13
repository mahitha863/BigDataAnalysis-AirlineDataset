package com.proj.secsortTripsByDistance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyWritable implements WritableComparable{
	
	String route;
	Long distance;
	
    public CompositeKeyWritable() {
    	
    }

	public CompositeKeyWritable(String route, Long distance) {
		super();
		this.route = route;
		this.distance = distance;
	}

	public String getRoute() {
		return route;
	}

	public void setRoute(String route) {
		this.route = route;
	}

	public Long getDistance() {
		return distance;
	}

	public void setDistance(Long distance) {
		this.distance = distance;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		route = in.readUTF();
		distance = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(route);
		out.writeLong(distance);
	}

	public int compareTo(Object o) {
        String thisValue = this.getRoute();
        String thatValue = ((CompositeKeyWritable)o).getRoute();
        int result = thisValue.compareTo(thatValue);
        return (result < 0 ? -1 : (result == 0 ? 0 : 1));
      }
	
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return route + " " + distance;
	}

}

package com.neshant.tutorial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

	// month + "," + startStationID + "," + EndStationID +","+ count
	private int month;
	private int count;
	private String route;

	public CompositeKeyWritable() {

	}

	public CompositeKeyWritable(int m, int c, String r) {
		this.month = m;
		this.count = c;
		this.route = r;

	}

	public int compareTo(CompositeKeyWritable o) {
		int result = new Integer(o.getMonth()).compareTo(new Integer(o.getMonth()));
		if (0 == result) {
			result = -1* new Integer(o.getCount()).compareTo(new Integer(o.getCount()));
		}
return result;

	}	
	

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getRoute() {
		return route;
	}

	public void setRoute(String route) {
		this.route = route;
	}

	public void write(DataOutput d) throws IOException {
		d.writeInt(month);
		d.writeInt(count);
		WritableUtils.writeString(d, route);

	}

	public void readFields(DataInput di) throws IOException {
		month = di.readInt();
		count = di.readInt();
		route = WritableUtils.readString(di);

	}

	public String toString() {
		return (new StringBuilder().append(month).append("\t").append(count).append("\t").append(route).toString());
	}

}

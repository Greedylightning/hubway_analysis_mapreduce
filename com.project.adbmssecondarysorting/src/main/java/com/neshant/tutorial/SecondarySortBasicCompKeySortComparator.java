package com.neshant.tutorial;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortBasicCompKeySortComparator extends WritableComparator {

	protected SecondarySortBasicCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = new Integer(key1.getMonth()).compareTo(new Integer(key1.getMonth()));
		if (cmpResult == 0)// same deptNo
		{
			return -1 * new Integer(key1.getCount()).compareTo(new Integer(key1.getCount()));
			// If the minus is taken out, the values will be in
			// ascending order
		}
		return cmpResult;
	}
}

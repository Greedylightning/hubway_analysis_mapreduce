package com.neshant.tutorial;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortBasicGroupingComparator extends WritableComparator {
	  protected SecondarySortBasicGroupingComparator() {
			super(CompositeKeyWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
			CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
			return new Integer(key1.getMonth()).compareTo(new Integer(key1.getMonth()));
		}
	}
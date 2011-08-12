package com.lifexweb.app.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;

public class UserIdComparator extends WritableComparator {

	public UserIdComparator() {
		super(LogKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		//UserIdで比較
		LogKeyWritable lkw1 = (LogKeyWritable)wc1;
		LogKeyWritable lkw2 = (LogKeyWritable)wc2;
		return lkw1.getUserId().compareTo(lkw2.getUserId());
	}
}

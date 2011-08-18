package com.lifexweb.app.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;

public class LogSortComparator extends WritableComparator {

	public LogSortComparator() {
		super(LogKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		
		LogKeyWritable lkw1 = (LogKeyWritable)wc1;
		LogKeyWritable lkw2 = (LogKeyWritable)wc2;
		
		//UserIdで比較
		int cmp = lkw1.getUserId().compareTo(lkw2.getUserId());
		if (cmp != 0) {
			return cmp;
		}
		return lkw1.getLogDatetime().compareTo(lkw2.getLogDatetime());
	}
}

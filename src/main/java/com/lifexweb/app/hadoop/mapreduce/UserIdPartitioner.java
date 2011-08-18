package com.lifexweb.app.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;
import com.lifexweb.app.hadoop.writable.LogValueWritable;

public class UserIdPartitioner extends Partitioner<LogKeyWritable, LogValueWritable> {
	
	@Override
	public int getPartition(LogKeyWritable key, LogValueWritable value, int numReducer) {
		//UserIdで同じ所に行くようにする
		return (key.getUserId().hashCode() * 163 & Integer.MAX_VALUE) % numReducer;
	}
}

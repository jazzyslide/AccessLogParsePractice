package com.lifexweb.app.hadoop.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;

public class UserIdPartitioner extends Partitioner<LogKeyWritable, NullWritable> {
		
	@Override
	public int getPartition(LogKeyWritable key, NullWritable value, int numReducer) {
		//UserIdで同じ所に行くようにする
		return (key.getUserId().hashCode() * 163 & Integer.MAX_VALUE) % numReducer;
	}
}

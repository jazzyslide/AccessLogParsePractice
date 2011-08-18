package com.lifexweb.app.hadoop.writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class LogKeyWritable implements WritableComparable<LogKeyWritable> {

	private Long logDatetime;
	private String userId;
	
	public LogKeyWritable() {
		set(0L, new String());
	}
	
	public LogKeyWritable(Long logDatetime, String userId) {
		set(logDatetime, userId);
	}

	public void set(Long logDatetime, String userId) {
		this.logDatetime = logDatetime;
		this.userId = userId;
	}
	
	public void set(LogKeyWritable logKey) {
		this.logDatetime = logKey.getLogDatetime();
		this.userId = logKey.getUserId();	
	}
	
	public Long getLogDatetime() {
		return logDatetime;
	}

	public void setLogDatetime(Long logDatetime) {
		this.logDatetime = logDatetime;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVLong(out, logDatetime);
		WritableUtils.writeString(out, userId);
	}

	public void readFields(DataInput in) throws IOException {
		logDatetime = WritableUtils.readVLong(in);
		userId = WritableUtils.readString(in);
	}

	public int compareTo(LogKeyWritable cmpLine) {
		int cmp = userId.compareTo(cmpLine.getUserId());
		if (cmp != 0) {
			return cmp;
		}
		return logDatetime.compareTo(cmpLine.getLogDatetime());
	}
	
	@Override
	public String toString() {
		return logDatetime + "\t" + userId;
	}

}

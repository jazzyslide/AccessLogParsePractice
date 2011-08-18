package com.lifexweb.app.hadoop.writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class LogValueWritable implements WritableComparable<LogValueWritable> {

	private String logDatetime;
	private String userId;
	private Integer urlId;
	private Integer cvId;
	
	public LogValueWritable() {
		set(new String(), new String(), 0, 0);
	}
	
	public LogValueWritable(String logDatetime, String userId, Integer urlId, Integer cvId) {
		set(logDatetime, userId, urlId, cvId);
	}

	public void set(String logDatetime, String userId, Integer urlId, Integer cvId) {
		this.logDatetime = logDatetime;
		this.userId = userId;
		this.urlId = urlId;
		this.cvId = cvId;
	}
	
	public void set(LogValueWritable logKey) {
		this.logDatetime = logKey.getLogDatetime();
		this.userId = logKey.getUserId();
		this.urlId = logKey.getUrlId();
		this.cvId = logKey.getCvId();		
	}
	
	public String getLogDatetime() {
		return logDatetime;
	}

	public void setLogDatetime(String logDatetime) {
		this.logDatetime = logDatetime;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Integer getUrlId() {
		return urlId;
	}

	public void setUrlId(Integer urlId) {
		this.urlId = urlId;
	}

	public Integer getCvId() {
		return cvId;
	}

	public void setCvId(Integer cvId) {
		this.cvId = cvId;
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, logDatetime);
		WritableUtils.writeString(out, userId);
		WritableUtils.writeVInt(out, urlId);
		WritableUtils.writeVInt(out, cvId);
	}

	public void readFields(DataInput in) throws IOException {
		logDatetime = WritableUtils.readString(in);
		userId = WritableUtils.readString(in);
		urlId = WritableUtils.readVInt(in);
		cvId = WritableUtils.readVInt(in);
	}
	
	public int compareTo(LogValueWritable cmpLine) {
		int cmp = userId.compareTo(cmpLine.getUserId());
		if (cmp != 0) {
			return cmp;
		}
		cmp = logDatetime.compareTo(cmpLine.getLogDatetime());
		if (cmp != 0) {
			return cmp;
		}
		cmp = urlId.compareTo(cmpLine.getUrlId());
		if (cmp != 0) {
			return cmp;
		}
		return cvId.compareTo(cmpLine.getCvId());
	}
	
	@Override
	public String toString() {
		return logDatetime + "\t" + userId + "\t" + urlId + "\t" + cvId;
	}
}

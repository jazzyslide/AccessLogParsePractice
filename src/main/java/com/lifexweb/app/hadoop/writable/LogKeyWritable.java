package com.lifexweb.app.hadoop.writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class LogKeyWritable implements WritableComparable<LogKeyWritable> {

	private Long logDatetime;
	private String userId;
	private String urlId;
	private String cvId;
	
	public LogKeyWritable() {
		set(new Long(0), new String(), new String(), new String());
	}
	
	public LogKeyWritable(Long logDatetime, String userId, String urlId, String cvId) {
		set(logDatetime, userId, urlId, cvId);
	}

	public void set(Long logDatetime, String userId, String urlId, String cvId) {
		this.logDatetime = logDatetime;
		this.userId = userId;
		this.urlId = urlId;
		this.cvId = cvId;
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

	public String getUrlId() {
		return urlId;
	}

	public void setUrlId(String urlId) {
		this.urlId = urlId;
	}

	public String getCvId() {
		return cvId;
	}

	public void setCvId(String cvId) {
		this.cvId = cvId;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(logDatetime);
		out.writeUTF(userId);
		out.writeUTF(urlId);
		out.writeUTF(cvId);
	}

	public void readFields(DataInput in) throws IOException {
		logDatetime = in.readLong();
		userId = in.readUTF();
		urlId = in.readUTF();
		cvId = in.readUTF();
	}

	//TODO 一旦まったく同時刻のURLアクセスとCVの場合は考慮しない
	public int compareTo(LogKeyWritable cmpLine) {
		int cmp = userId.compareTo(cmpLine.getUserId());
		if (cmp != 0) {
			return cmp;
		}
		return logDatetime.compareTo(cmpLine.getLogDatetime()) * -1;
	}
	
	@Override
	public String toString() {
		return logDatetime + "\t" + userId + "\t" + urlId + "\t" + cvId;
	}

}

package com.lifexweb.app.hadoop.writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class LogKeyWritable implements WritableComparable<LogKeyWritable> {

	private Long logDatetime;
	private String userId;
	private int urlId;
	private int cvId;
	
	public LogKeyWritable() {
		set(0L, new String(), 0, 0);
	}
	
	public LogKeyWritable(Long logDatetime, String userId, int urlId, int cvId) {
		set(logDatetime, userId, urlId, cvId);
	}

	public void set(Long logDatetime, String userId, int urlId, int cvId) {
		this.logDatetime = logDatetime;
		this.userId = userId;
		this.urlId = urlId;
		this.cvId = cvId;
	}
	
	public void set(LogKeyWritable logKey) {
		this.logDatetime = logKey.getLogDatetime();
		this.userId = logKey.getUserId();
		this.urlId = logKey.getUrlId();
		this.cvId = logKey.getCvId();		
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

	public int getUrlId() {
		return urlId;
	}

	public void setUrlId(int urlId) {
		this.urlId = urlId;
	}

	public int getCvId() {
		return cvId;
	}

	public void setCvId(int cvId) {
		this.cvId = cvId;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(logDatetime);
		out.writeUTF(userId);
		out.writeInt(urlId);
		out.writeInt(cvId);
	}

	public void readFields(DataInput in) throws IOException {
		logDatetime = in.readLong();
		userId = in.readUTF();
		urlId = in.readInt();
		cvId = in.readInt();
	}

	/**
	 * 同じuserId単位で時刻が新しいものから（降順）並べ替え
	 * まったく同時刻のURLアクセスとCVの場合はログの書きこまれている順に処理する
	 * (同時刻でもURL -> CV の順で書きこまれていればカウントできるようにする)
	 */
	public int compareTo(LogKeyWritable cmpLine) {
		int cmp = userId.compareTo(cmpLine.getUserId());
		if (cmp != 0) {
			return cmp;
		}
		
		cmp = logDatetime.compareTo(cmpLine.getLogDatetime()) * -1;
		if (cmp != 0) {
			return cmp;
		}
		return 1;			
	}
	
	@Override
	public String toString() {
		return logDatetime + "\t" + userId + "\t" + urlId + "\t" + cvId;
	}

}

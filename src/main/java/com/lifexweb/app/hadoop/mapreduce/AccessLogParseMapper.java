package com.lifexweb.app.hadoop.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;

public class AccessLogParseMapper extends Mapper<LongWritable, Text, LogKeyWritable, NullWritable> {
	private LogKeyWritable logKey = new LogKeyWritable();
	private Date logDatetime;
	private SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private int urlId;
	private int cvId;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//TODO logging
		
		String line = value.toString();
		String[] logElements = line.split("\\t");
		
		//正しいデータ数かチェックする
		if (logElements.length > 4) {
			//TODO logging
			return;
		}
		try {
			logDatetime = fmt.parse(logElements[0]);
			urlId = Integer.parseInt(logElements[2]);
			cvId = Integer.parseInt(logElements[3]);
			
			//urlIdとcvIdが「どちらも0以下」や「どちらも正の数」は弾く
			if ((urlId <= 0 && cvId <= 0) || (urlId > 0 && cvId > 0)) {
				//TODO logging
				return;
			}
		} catch (ParseException e) {
			//TODO logging
			return;
		}
		
		logKey.set(logDatetime.getTime(), logElements[1], urlId, cvId);
		context.write(logKey, NullWritable.get());
	}
}

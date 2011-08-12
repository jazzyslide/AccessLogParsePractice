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
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] logElements = line.split("\\t");
		
		//TODO 各要素が正しいデータ、データ数かチェックする
		try {
			logDatetime = fmt.parse(logElements[0]);
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}
		
		logKey.set(logDatetime.getTime(), logElements[1], logElements[2], logElements[3]);
		context.write(logKey, NullWritable.get());
	}
}

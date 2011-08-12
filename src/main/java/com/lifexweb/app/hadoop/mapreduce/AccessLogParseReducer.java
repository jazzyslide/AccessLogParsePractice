package com.lifexweb.app.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;

public class AccessLogParseReducer extends Reducer<LogKeyWritable, NullWritable, Text, NullWritable> {

	private Text resultKey = new Text();
    private SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Date logDatetime = new Date();
	
	@Override
	protected void reduce(LogKeyWritable logKey, Iterable<NullWritable> value, Context context)
			throws IOException, InterruptedException {
		
		for (NullWritable nw : value) {
			
			//debug
			logDatetime.setTime(logKey.getLogDatetime());
			resultKey.set(fmt.format(logDatetime) + "\t" + logKey.getUserId() + "\t" + 
					logKey.getUrlId() + "\t" + logKey.getCvId());
			context.write(resultKey, NullWritable.get());
		}
		context.write(new Text("-------------"), NullWritable.get());
	}	
}

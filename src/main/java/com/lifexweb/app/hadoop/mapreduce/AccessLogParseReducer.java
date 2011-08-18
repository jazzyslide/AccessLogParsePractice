package com.lifexweb.app.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;
import com.lifexweb.app.hadoop.writable.LogValueWritable;

public class AccessLogParseReducer extends Reducer<LogKeyWritable, LogValueWritable, NullWritable, Text> {

	private Text resultValue = new Text();
    private LogValueWritable tmpUrlLogValue = new LogValueWritable();
	
	private static Log log = LogFactory.getLog(AccessLogParseReducer.class);
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[REDUCE] reduce task started... JobId: " + context.getJobID().getId());
	}

	@Override
	protected void reduce(LogKeyWritable logKey, Iterable<LogValueWritable> logValues, Context context)
			throws IOException, InterruptedException {
		
		for (LogValueWritable value : logValues) {
			if (value.getUrlId() > 0) {
				tmpUrlLogValue.set(value);
				
			} else if (tmpUrlLogValue.getUserId().equals(value.getUserId()) && value.getCvId() > 0) {
				resultValue.set(tmpUrlLogValue.getLogDatetime() + "\t" +
							value.getLogDatetime() + "\t" + 
							tmpUrlLogValue.getUserId() + "\t" +
							tmpUrlLogValue.getUrlId() + "\t" +
							value.getCvId());
				context.write(NullWritable.get(), resultValue);
			}	
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[REDUCE] reduce task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}

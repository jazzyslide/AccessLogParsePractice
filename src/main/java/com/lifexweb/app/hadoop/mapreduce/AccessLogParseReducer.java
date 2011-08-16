package com.lifexweb.app.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.lifexweb.app.hadoop.writable.LogKeyWritable;

public class AccessLogParseReducer extends Reducer<LogKeyWritable, NullWritable, Text, NullWritable> {

	private Text resultKey = new Text();
    private SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private LogKeyWritable tmpLogKey = new LogKeyWritable();
	
	private static Log log = LogFactory.getLog(AccessLogParseReducer.class);
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[REDUCE] reduce task started... JobId: " + context.getJobID().getId());
	}

	@Override
	protected void reduce(LogKeyWritable logKey, Iterable<NullWritable> value, Context context)
			throws IOException, InterruptedException {
		
		for (@SuppressWarnings("unused") NullWritable nw : value) {
			if (tmpLogKey.getLogDatetime() == 0 || isUrlLog(tmpLogKey)) {
				tmpLogKey.set(logKey);
			} else if (!isUrlLog(tmpLogKey)) {
				if (isUrlLog(logKey)) {
					resultKey.set(fmt.format(logKey.getLogDatetime()) + "\t" + 
									fmt.format(tmpLogKey.getLogDatetime()) + "\t" + 
									logKey.getUserId() + "\t" + 
									logKey.getUrlId() + "\t" + 
									tmpLogKey.getCvId());
					context.write(resultKey, NullWritable.get());
				}
				tmpLogKey.set(logKey);
			}	
		}
	}	

	private boolean isUrlLog(LogKeyWritable log) {
		return (log.getUrlId() > 0 && log.getCvId() == 0);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[REDUCE] reduce task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}

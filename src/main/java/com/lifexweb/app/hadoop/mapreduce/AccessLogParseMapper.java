package com.lifexweb.app.hadoop.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	
	private static Log log = LogFactory.getLog(AccessLogParseMapper.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[MAP] map task started... JobId: " + context.getJobID().getId());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] logElements = line.split("\\t");
		
		//正しいデータ数かチェックする
		if (logElements.length > 4) {
			log.warn("[MAP] SKIP : Invalid Num of Elements:["+ context.getCurrentValue() + "]");
			return;
		}
		try {
			logDatetime = fmt.parse(logElements[0]);
			urlId = Integer.parseInt(logElements[2]);
			cvId = Integer.parseInt(logElements[3]);
			
			//urlIdとcvIdが「どちらも0以下」や「どちらも正の数」は弾く
			if ((urlId <= 0 && cvId <= 0) || (urlId > 0 && cvId > 0)) {
				log.warn("[MAP] SKIP : Invalid match of urlId and cvId:[" + context.getCurrentValue() + "]");
				return;
			}
		} catch (ParseException e) {
			log.warn("[MAP] SKIP : Parse Error:[" + context.getCurrentValue() + "]");
			return;
		} catch (NumberFormatException e) {
			log.warn("[MAP] SKIP : NumberFormatException:[" + context.getCurrentValue() + "]");
			return;
		}
		
		logKey.set(logDatetime.getTime(), logElements[1], urlId, cvId);
		context.write(logKey, NullWritable.get());
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[MAP] map task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}

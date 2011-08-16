package com.lifexweb.app.hadoop.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.lifexweb.app.hadoop.mapreduce.AccessLogParseMapper;
import com.lifexweb.app.hadoop.mapreduce.AccessLogParseReducer;
import com.lifexweb.app.hadoop.mapreduce.UserIdComparator;
import com.lifexweb.app.hadoop.mapreduce.UserIdPartitioner;
import com.lifexweb.app.hadoop.writable.LogKeyWritable;

/**
 * @author kato_hideya
 * AccessLogParsePracticeの実行用Mainクラス
 */
public class AccessLogParsePracticeMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		
		Job job = new Job(conf, "AccessLogParsePractice");
		job.setJarByClass(AccessLogParsePracticeMain.class);
		
		job.setMapperClass(AccessLogParseMapper.class);
		job.setPartitionerClass(UserIdPartitioner.class);
		job.setGroupingComparatorClass(UserIdComparator.class);
		job.setReducerClass(AccessLogParseReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(LogKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		if (success) System.out.println("Job SUCCESS!!!");
	}

}

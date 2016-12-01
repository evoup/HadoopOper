package com.huike.action01;

import java.io.IOException;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Average extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(Average.class);

	public static class AverageCountMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			logger.debug("[AverageCountMapper][line:"+ line + "]");
			String[] parameters = line.split("\\s+");
			logger.debug("[AverageCountMapper][parameters:" + new Gson().toJson(parameters) + "]");
			context.write(new Text(parameters[0]), new Text(parameters[1]));

		}

	}

	public static class AverageCountCombiner extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			logger.debug("[AverageCountCombiner]");
			Double sum = 0.00;
			int count = 0;
			for (Text item : values) {
				sum = sum + Double.parseDouble(item.toString());
				logger.debug("[AverageCountCombiner][sum:" + sum + "]");
				count++;
			}
			context.write(new Text(key), new Text(sum + "-" + count));
		}
	}

	public static class AverageCountReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			logger.debug("[AverageCountReducer]");
			Double sum = 0.00;
			int count = 0;
			for (Text t : values) {
				logger.debug("[AverageCountReducer][Text:" + t + "]");
				String[] str = t.toString().split("-");
				logger.debug("[AverageCountReducer][str:" +new Gson().toJson(str) + "]");
				sum += Double.parseDouble(str[0]);
				count += Integer.parseInt(str[1]);
				logger.debug("[AverageCountReducer][sum:" + sum + "][count:" + count + "]");
			}
			double average = sum / count;
			logger.debug("[AverageCountReducer][average:" + average + "]");
			context.write(new Text(key), new Text(String.valueOf(average)));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "Average");
		job.setMapperClass(AverageCountMapper.class);
		job.setReducerClass(AverageCountReducer.class);
		job.setCombinerClass(AverageCountCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(Average.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action01/Average.txt", "/test/action01/output/" };
		int res = ToolRunner.run(new Configuration(), new Average(), args0);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(res);
		logger.debug("[main][res:" + res + "]");
	}
}

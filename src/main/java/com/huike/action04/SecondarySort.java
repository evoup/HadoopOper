package com.huike.action04;

import java.io.IOException;
import java.util.StringTokenizer;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SecondarySort extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(SecondarySort.class);

	public static class SecondarySortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
		private IntPair intkey = new IntPair();
		private IntWritable intvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			LOG.info("[SecondarySortMapper][line:" + line + "]");
			StringTokenizer tokenizer = new StringTokenizer(line);
			int left = 0;
			int right = 0;
			if (tokenizer.hasMoreTokens()) {
				LOG.info("[SecondarySortMapper][hasMoreTokens]");
				left = Integer.parseInt(tokenizer.nextToken());
				LOG.info("[SecondarySortMapper][left:" + left + "]");
				if (tokenizer.hasMoreTokens()) {
					LOG.info("[SecondarySortMapper][right:" + right + "]");
					right = Integer.parseInt(tokenizer.nextToken());
				}
				LOG.info("[SecondarySortMapper][left:" + left + "][right:" + right + "]");
				intkey.set(left, right);
				intvalue.set(right);
				context.write(intkey, intvalue);
			}
		}
	}

	public static class SecondarySortReducer extends Reducer<IntPair, IntWritable, Text, IntWritable> {
		private final Text left = new Text();

		public void reduce(IntPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			left.set(Integer.toString(key.getFirst()));
			LOG.info("[SecondarySortReducer][left set:" + Integer.toString(key.getFirst()) + "]");
			for (IntWritable val : values) {
				LOG.info("[SecondarySortReducer][val:" + val + "]");
				context.write(left, val);
			}
		}
	}

	/**
	 * 分区类, 根据first确定Partition
	 */
	public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {
		@Override
		public int getPartition(IntPair key, IntWritable value, int numPartitions) {
			LOG.info("[FirstPartitioner][getPartition][numPartitions:" + numPartitions + "]" +
					"[key.getFirst:" + key.getFirst() + "][result:" +
					Math.abs(key.getFirst() * 1127) % numPartitions + "]");
			return Math.abs(key.getFirst() * 1127) % numPartitions;
		}
	}

	/**
	 * 继承WritableComparator
	 */
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(IntPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		// Compare two WritableComparables.
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			LOG.info("[GroupingComparator][ip1:" + new Gson().toJson(ip1) + "][ip2:" + new Gson().toJson(ip2) + "]");
			int l = ip1.getFirst();
			int r = ip2.getFirst();
			LOG.info("[GroupingComparator][l:" + l + "][r:" + r + "]");
			if (l==r) {
				LOG.info("[GroupingComparator][return:0]");
			} else {
				if (l < r) {
					LOG.info("[GroupingComparator][return:-1]");
				} else {
					LOG.info("[GroupingComparator][return:1]");
				}
			}
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "SecondarySort");
		job.setJarByClass(SecondarySort.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		job.setMapperClass(SecondarySortMapper.class);// Mapper
		job.setReducerClass(SecondarySortReducer.class);// Reducer

		job.setPartitionerClass(FirstPartitioner.class);// 分区函数
		job.setNumReduceTasks(2);
		// job.setSortComparatorClass(KeyComparator.Class);//使用IntPair自带的排序
		job.setGroupingComparatorClass(GroupingComparator.class);// 分组函数

		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action04/SecondarySort.txt", "/test/action04/output/" };
		int res = ToolRunner.run(new Configuration(), new SecondarySort(), args0);
		System.out.println(res);

	}
}

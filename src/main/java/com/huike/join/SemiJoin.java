package com.huike.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// semi join: https://mariadb.com/kb/en/mariadb/semi-join-subquery-optimizations/
public class SemiJoin extends Configured implements Tool {
	private final static String STATION_FILE = "Station.txt";
	private final static String TEMPERATURE_FILE = "Temperature.txt";
	private static final Log LOG = LogFactory.getLog(SemiJoin.class);

	public static class SemiJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Set<String> joinKeys = new HashSet<>();
		private Text joinKey = new Text();
		private Text combineValue = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			LOG.info("[SemiJoinMapper][setup]");
			BufferedReader br;
			String station;
			Path[] paths = context.getLocalCacheFiles();
			for (Path path : paths) {
				String pathStr = path.toString();
				LOG.info("[SemiJoinMapper][setup][pathStr:" + pathStr + "]");
				if (pathStr.endsWith("Station.txt")) {
					LOG.info("[SemiJoinMapper][setup][STATION_FILE]");
					br = new BufferedReader(new FileReader(pathStr));
					while (null != (station = br.readLine())) {
						String[] stationItems = station.split("\\s+");
						LOG.info("[SemiJoinMapper][setup][stationItems:" + new Gson().toJson(stationItems) +"]");
						if (stationItems.length == 3) {
							joinKeys.add(stationItems[0]);
							LOG.info("[SemiJoinMapper][setup][joinKeys add:" + stationItems[0] + "]");
						}
					}
				}
			}
			LOG.info("[SemiJoinMapper][setup][joinKeys:" + new Gson().toJson(joinKeys) + "]");
		}

		/**
		 * map直接判断当前记录的stationId在缓存中是否存在
		 */
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			LOG.info("[SemiJoinMapper][map][pathName:" + pathName + "]");
			// 如果数据来自于STATION_FILE，加一个STATION_FILE的标记
			if (pathName.endsWith(STATION_FILE)) {
				String[] valueItems = value.toString().split("\\s+");
				LOG.info("[SemiJoinMapper][map][STATION_FILE][valueItems:" + new Gson().toJson(valueItems) + "]");
				// 过滤掉脏数据
				if (valueItems.length != 3) {
					return;
				}
				// 过滤掉无法join的记录
				if (joinKeys.contains(valueItems[0])) {
					joinKey.set(valueItems[0]);
					combineValue.set(STATION_FILE + valueItems[1] + "\t" + valueItems[2]);
				}
			} else if (pathName.endsWith(TEMPERATURE_FILE)) {
				// 如果数据来自于TEMPERATURE_FILE，加一个TEMPERATURE_FILE的标记
				String[] valueItems = value.toString().split("\\s+");
				LOG.info("[SemiJoinMapper][map][TEMPERATURE_FILE][valueItems:" + new Gson().toJson(valueItems) + "]");
				// 过滤掉脏数据
				if (valueItems.length != 3) {
					return;
				}
				// 过滤掉无法join的记录
				if (joinKeys.contains(valueItems[0])) {
					joinKey.set(valueItems[0]);
					combineValue.set(TEMPERATURE_FILE + valueItems[1] + "\t" + valueItems[2]);
				}
			}
			context.write(joinKey, combineValue);
			LOG.info("[SemiJoinMapper][map][context.write][key:" + joinKey + "][value:" + combineValue + "]");
		}
	}

	public static class SemiJoinReducer extends Reducer<Text, Text, Text, Text> {
		private List<String> stations = new ArrayList<String>();
		private List<String> temperatures = new ArrayList<String>();
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 一定要清空数据
			stations.clear();
			temperatures.clear();
			LOG.info("[SemiJoinReducer][reduce][key:" + key + "][clean stations and temperatures]");
			// 相同key的记录会分组到一起，我们需要把相同key下来自于不同文件的数据分开
			for (Text value : values) {
				String val = value.toString();
				if (val.startsWith(STATION_FILE)) {
					stations.add(val.replaceFirst(STATION_FILE, ""));
					LOG.info("[SemiJoinReducer][reduce][key:" + key + "][stations add][station:" + val.replaceFirst(STATION_FILE, "") + "]");
				} else if (val.startsWith(TEMPERATURE_FILE)) {
					temperatures.add(val.replaceFirst(TEMPERATURE_FILE, ""));
					LOG.info("[SemiJoinReducer][reduce][key:" +key + "][temperatures add][temperature:" + val.replaceFirst(TEMPERATURE_FILE, "") + "]");
				}
			}

			for (String station : stations) {
				for (String temperature : temperatures) {
					result.set(station + "\t" + temperature);
					context.write(key, result);
					LOG.info("[SemiJoinReducer][reduce][context.write][key:" + key + "][value:" + result + "]");
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		// if (otherArgs.length < 2) {
		// System.err.println("Usage: semijoin <cachePath> <in> [<in>...]
		// <out>");
		// System.exit(2);
		// }
		Path mypath = new Path(args[3]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "semijoin");
		// 把小表缓存起来
		job.addCacheFile(new Path(args[0]).toUri());
		job.setJarByClass(SemiJoin.class);
		job.setMapperClass(SemiJoinMapper.class);
		job.setReducerClass(SemiJoinReducer.class);
		// job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// for (int i = 1; i < otherArgs.length - 1; ++i) {
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));
		// }
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/join/Station.txt", "/test/join/Station.txt", "/test/join/Temperature.txt",
				"/test/join/output/" };
		int res = ToolRunner.run(new Configuration(), new SemiJoin(), args0);
		System.out.println(res);

	}
}

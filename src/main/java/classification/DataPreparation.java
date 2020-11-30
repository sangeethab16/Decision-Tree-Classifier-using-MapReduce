package classification;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DataPreparation extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(DataPreparation.class);

	public static class AttributeMapper extends Mapper<Object, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			int rowId = Integer.parseInt(tokens[0]);
			String className = tokens[tokens.length - 1];
			for(int i= 1; i < tokens.length - 1; i++) {
				context.write(new Text(tokens[i]), new Text(rowId + "," + className));
			}
		}
	}

	public static class AttributeReducer extends Reducer<Text, Text, Text, Text> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			int cnt = 0;

			for(Text value: values) {
				cnt += 1;
			}

			result.set(cnt);
			context.write(key, new Text("cnt"));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(DataPreparation.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(AttributeMapper.class);
//		job.setCombinerClass(AttributeReducer.class);
		job.setReducerClass(AttributeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new DataPreparation(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
package classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DatasetSplit extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(DatasetSplit.class);

	public static class SplitMapper extends Mapper<Object, Text, IntWritable, Text> {
		private final IntWritable result = new IntWritable();
		int ak;
		double cpk;

		@Override
		public void setup(Context context) {
			ak = Integer.parseInt(context.getConfiguration().get("ak"));
			cpk = Double.parseDouble(context.getConfiguration().get("cpk"));
		}
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String[] row = value.toString().split(",");
			float[] rowValues = new float[row.length];
			for(int i=0;i< row.length; i++)
				rowValues[i] = float.parseFloat(row[i]);
			int id = 0;
			if(rowValues[ak] > cpk)
				id = 1;
			else
				id = 0;
			context.write(new IntWritable(id),value);
			}

		}
	public static class SplitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			int k = 2;
			float[] prob = new float[k];
			int n = 0;
			for( Text val: values)
				n += 1;
			// Should implement the algorithm

			result.set();
			context.write(key, new Text());
		}


	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(DatasetSplit.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(ComputeMapper.class);
		job.setCombinerClass(ComputeReducer.class);
		job.setReducerClass(ComputeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
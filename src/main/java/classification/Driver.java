package classification;

import java.io.Console;
import java.io.IOException;

import classification.utility.SPLIT_COUNTER;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Driver extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Driver.class);

	@Override
	public int run(final String[] args) throws Exception {

		int maxHeight = 1;
		int height = 0;

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1] + "/0");


		while(height < maxHeight) {
			Configuration conf = getConf();
			Job job = Job.getInstance(conf, "Decision Tree");
			job.setJarByClass(Driver.class);
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", ",");

			job.setMapperClass(AttributeSelectionMapper.class);
			job.setReducerClass(SelectReducer.class);


			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(SelectMapperWritable.class);

			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, new Path("tempo"));

			job.waitForCompletion(true);

			long cutpoint = job.getCounters().findCounter(SPLIT_COUNTER.CUTPOINT).getValue();
			double finalCutpoint = (double) cutpoint / 100000;
			System.out.println(finalCutpoint);

			long selectedAttribute = job.getCounters().findCounter(SPLIT_COUNTER.ATTRIBUTE_COLUMN).getValue();
			System.out.println(selectedAttribute);





			Configuration confTwo = getConf();
			Job jobTwo = Job.getInstance(confTwo, "Decision Tree Building");
			jobTwo.setJarByClass(Driver.class);
			Configuration jobConfigTwo = jobTwo.getConfiguration();
			jobConfigTwo.set("mapreduce.output.textoutputformat.separator", ",");

			jobConfigTwo.setLong("selectedAttribute", selectedAttribute);
			jobConfigTwo.setDouble("selectedAttributeCutPoint", finalCutpoint);
			jobConfigTwo.setFloat("minProbability", 0.90f);
			jobConfigTwo.setInt("maxRecordsInPartition", 2);

			jobTwo.setMapperClass(SplitMapper.class);
			jobTwo.setReducerClass(SplitReducer.class);

			FileInputFormat.addInputPath(jobTwo, inputPath);
			FileOutputFormat.setOutputPath(jobTwo, outputPath);

			MultipleOutputs.addNamedOutput(jobTwo, "tree", TextOutputFormat.class, LongWritable.class, Text.class);

			MultipleOutputs.addNamedOutput(jobTwo, "data", TextOutputFormat.class, Text.class, Text.class);

			jobTwo.waitForCompletion(true);



			height++;

		}




		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new Driver(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}

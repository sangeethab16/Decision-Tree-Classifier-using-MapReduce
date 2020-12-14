package classification;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PredictionDriver extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PredictionDriver.class);
	private static final String FILE_LABEL = "Tree";

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Evaluate");
		job.setJarByClass(PredictionDriver.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		job.setMapperClass(PredictionMapper.class);
		job.setReducerClass(AccuracyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		int i = 0;
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
	            new Path(args[2]), true);		//Iterates all the files in directory recursively
		while(fileStatusListIterator.hasNext()){
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			
			if (fileStatus.getPath().toString().contains("decision")) {
				System.out.println(fileStatus.getPath().toString());
				job.addCacheFile(new URI(fileStatus.getPath().toString() + "#" + FILE_LABEL + (i++))); //Cache file is generated for every iteration
			}

		}
		job.getConfiguration().setInt("FilesTotal", i);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "PREDICTION", TextOutputFormat.class, Text.class, Text.class);
		job.waitForCompletion(true);

		Counters cn = job.getCounters();
		Long trueCounter = cn.findCounter(CounterEnum.TRUE).getValue();
		Long totalCounter = cn.findCounter(CounterEnum.TOTAL).getValue();

		System.out.println("ACCURACY : " + (float) (trueCounter * 100) / totalCounter);
		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new PredictionDriver(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

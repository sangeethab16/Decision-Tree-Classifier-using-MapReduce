package classification;

import java.io.Console;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

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

public class Driver extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Driver.class);

	long partitions = -1;

	@Override
    	public int run(final String[] args) throws Exception {
		int maxHeight = 6;
        	int height = 0;
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1] + "/0");
		Path tempOutput = new Path(args[2]+ "/" + height);

		while(height < maxHeight) {
			Configuration conf = getConf();
			Job job = Job.getInstance(conf, "Decision Tree");
			job.setJarByClass(Driver.class);
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", ",");
			
			job.setMapperClass(AttributeSelectionMapper.class);
			job.setPartitionerClass(SplitPartitioner.class);
			job.setReducerClass(SelectReducer.class);
			job.setNumReduceTasks((int)Math.pow(2.0,((double) height)));

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(SelectMapperWritable.class);

 			job.setOutputKeyClass(IntWritable.class);
 			job.setOutputValueClass(Text.class);


 			FileInputFormat.addInputPath(job, inputPath);
 			FileOutputFormat.setOutputPath(job, tempOutput);

 			job.waitForCompletion(true);

            
			Configuration confTwo = getConf();

			Job jobTwo = Job.getInstance(confTwo, "Decision Tree Building");
			jobTwo.setJarByClass(Driver.class);
			Configuration jobConfigTwo = jobTwo.getConfiguration();
			jobConfigTwo.set("mapreduce.output.textoutputformat.separator", ",");

			jobConfigTwo.setFloat("minProbability", 0.90f);
			jobConfigTwo.setInt("maxRecordsInPartition", 2);

 			jobTwo.setMapperClass(SplitMapper.class);
 			jobTwo.setReducerClass(SplitReducer.class);
			jobTwo.setPartitionerClass(SplitPartitioner.class);



			jobTwo.setOutputKeyClass(Text.class);
			jobTwo.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobTwo, inputPath);
			FileOutputFormat.setOutputPath(jobTwo, outputPath);

			MultipleOutputs.addNamedOutput(jobTwo, "decision", TextOutputFormat.class, LongWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(jobTwo, "data", TextOutputFormat.class, NullWritable.class, Text.class);



			int i = 0;

			FileStatus[] fileStatus = FileSystem.get(new java.net.URI(args[2] +"/" + height),
					new Configuration()).listStatus(new Path(args[2] +"/" + height ));



//				FileStatus[] fileStatus = fs.listStatus(new Path(args[2]+ "/" + height));
				for(FileStatus status : fileStatus){
					jobTwo.addCacheFile(new URI(status.getPath().toString() + "#filelabel" + (i++)));
				}
			jobTwo.getConfiguration().setInt("FilesTotal", i);

//			jobTwo.addCacheFile(new URI ( args[2]+ "/" + height + "#filelabel"));


			jobTwo.waitForCompletion(true);

 			inputPath = new Path(args[1] +"/" +height +"/data");
            		height++;
            		outputPath = new Path(args[1] + "/" + height);
            		tempOutput = new Path(args[2]+ "/" + height);

        	}



		return 1;
	}
	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        	}
		
		try {
			ToolRunner.run(new Driver(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}

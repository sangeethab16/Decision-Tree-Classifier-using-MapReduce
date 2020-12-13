package classification;

import java.io.Console;
import java.io.IOException;

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
		int maxHeight = 2;
        	int height = 0;
		
		Path inputPath = new Path(args[0]);
        	Path outputPath = new Path(args[1] + "/0");
        	Path tempOutput = new Path("tempo"+ "/" + height);

        	while(height < maxHeight) {
            		Configuration conf = getConf();
            		Job job = Job.getInstance(conf, "Decision Tree");
            		job.setJarByClass(Driver.class);
            		final Configuration jobConf = job.getConfiguration();
            		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
			
			job.setMapperClass(AttributeSelectionMapper.class);
            		job.setReducerClass(SelectReducer.class);
            		job.setNumReduceTasks(1);

 			job.setOutputKeyClass(IntWritable.class);
            		job.setOutputValueClass(SelectMapperWritable.class);



 			FileInputFormat.addInputPath(job, inputPath);
            		FileOutputFormat.setOutputPath(job, tempOutput);



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
			jobTwo.setPartitionerClass(SplitPartitioner.class);

 			FileInputFormat.addInputPath(jobTwo, inputPath);
            		FileOutputFormat.setOutputPath(jobTwo, outputPath);
			
            		jobTwo.waitForCompletion(true);
            		partitions = jobTwo.getCounters().findCounter(SPLIT_COUNTER.PARTITIONS).getValue();
            		System.out.println("party" + partitions);
				jobTwo.getCounters().findCounter(SPLIT_COUNTER.PARTITIONS).setValue(0);

 			inputPath = new Path(args[1] +"/" +height +"/data");
            		height++;
            		outputPath = new Path(args[1] + "/" + height);
            		tempOutput = new Path("tempo"+ "/" + height);


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

//obtain random subsets of the data of different sizes for running experiments with on AWS
package classification; 

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataDiffSizes extends Configuration implements Tool{
	public static class DiffSizeMapper extends Mapper <Object, Text, LongWritable, NullWritableText>{
		//private Random randomNumber = new Random();
		private Double size;
	
		//calculates sampling percentage to use as threshold
		protected void setup(Context context) throws IOException, InterruptedException {
			String percentage = context.getConfiguration().get("sampling_percentage");
			samplingPercentage = Double.parseDouble(percentage) / 100.0;
		}
	
		@Override
		//compares randomly generated number with threshold to decide whether to keep value or not
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//if (randomNumber.nextDouble() < samplingPercentage) {
				context.write(NullWritable.get(), value);
			//}
		}
	}
	
	@Override
	public int run(final String[] args) throws Exception {
		args = new String[] { "Replace this string with Input Path location", "Replace this string with output Path location" };
		
// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
/* set the hadoop system parameter */
		
		System.setProperty("hadoop.home.dir", "Replace this string with hadoop home directory location");
		if (args.length != 2) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}

		Configuration conf = ConfigurationFactory.getInstance();
		conf.set("sampling_percentage","20");
		Job job = Job.getInstance(conf);
		job.setJarByClass(DataDiffSizes.class);
		job.setJobName("Sampling_Dataset");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(DiffSizeMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new DataDiffSizes(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}

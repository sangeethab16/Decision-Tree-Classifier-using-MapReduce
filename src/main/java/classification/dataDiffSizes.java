//obtain random subsets of the data of different sizes for running experiments with on AWS
package classification; 

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataDiffSizes extends Mapper<Object, Text, LongWritable, NullWritableText>{
	private Random randomNumber = new Random();
	private Double samplingPercentage;
	
	//calculates sampling percentage to use as threshold
	protected void setup(Context context) throws IOException, InterruptedException {
		String percentage = context.getConfiguration().get("sampling_percentage");
		samplingPercentage = Double.parseDouble(percentage) / 100.0;
	}
	
	@Override
	//compares randomly generated number with threshold to decide whether to keep value or not
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (randomNumber.nextDouble() < samplingPercentage) {
			context.write(NullWritable.get(), value);
		}
	}
}

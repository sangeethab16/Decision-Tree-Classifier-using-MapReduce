//obtain random subsets of the data of different sizes for running experiments with on AWS
package classification; 

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataDiffSizes extends Mapper<Object, Text, LongWritable, Text>{
	private Random randomNumber = new Random();
	private Double samplingPercentage;
	protected void setup(Context context) throws IOException, InterruptedException {
		String percentage = context.getConfiguration().get("sampling_percentage");
		samplingPercentage = Double.parseDouble(percentage) / 100.0;
	}
}

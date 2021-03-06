package classification;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;




public class AccuracyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	@Override
	public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
		
		for (final IntWritable val : values) {
			context.getCounter(CounterEnum.TOTAL).increment(1);
			if (key.get() == 1) {
				context.getCounter(CounterEnum.TRUE).increment(val.get());
			}
		}
	}
	
}


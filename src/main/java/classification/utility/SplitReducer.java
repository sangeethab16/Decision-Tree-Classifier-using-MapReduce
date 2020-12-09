package classification.utility;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SplitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private final IntWritable result = new IntWritable();

    @Override
    public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        int k = 2;
        float[] prob = new float[k];
        int n = 0;
        for (Text val : values)
            n += 1;
        // Algorithm to be implemented

//			context.write(key, new Text());
    }

}
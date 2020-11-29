package classification;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//input is of the form key = Ak, [(Xj/Ak, CutPoint Xj), (Xj+1/Ak, CutPoint Xj+1), (Xj+2/Ak, CutPoint Xj+2)]
public class SelectReducer extends Reducer<IntWritable, SelectMapperWritable, IntWritable, DoubleWritable> {

    @Override
    public void reduce(final IntWritable key, final Iterable<SelectMapperWritable> values, final Context context) throws IOException, InterruptedException {
        int sum = 0;
//        for (final IntWritable val : values) {
//            sum += val.get();
//        }
//        result.set(sum);
//        context.write(key, result);
    }

}

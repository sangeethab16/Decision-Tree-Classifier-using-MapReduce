package classification;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//input is of the form key = Ak, [(Xj/Ak, CutPoint Xj), (Xj+1/Ak, CutPoint Xj+1), (Xj+2/Ak, CutPoint Xj+2)]
public class SelectReducer extends Reducer<IntWritable, SelectMapperWritable, IntWritable, SelectMapperWritable> {
    
    @Override
    public void reduce(final IntWritable key, final Iterable<SelectMapperWritable> values, final Context context) throws IOException, InterruptedException {
        float ratio = 0.0f;
        int totalValueCount = 0;
        double cutPointSum = 0.0;

        for (final SelectMapperWritable val : values) {
            ratio += val.getRatio().get();
            cutPointSum += val.getAttributeValue().get();
            totalValueCount += 1;
        }

        double cutPointAvg = cutPointSum / totalValueCount ;

        context.write(key, new SelectMapperWritable(new FloatWritable(ratio), new DoubleWritable(cutPointAvg)));
    }

}

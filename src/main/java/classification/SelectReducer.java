package classification;

import classification.utility.RatioCutPoint;
import classification.utility.SPLIT_COUNTER;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


//input is of the form key = Ak, [(Xj/Ak, CutPoint Xj), (Xj+1/Ak, CutPoint Xj+1), (Xj+2/Ak, CutPoint Xj+2)]
public class SelectReducer extends Reducer<IntWritable, SelectMapperWritable, IntWritable, SelectMapperWritable> {

    Map<Integer, RatioCutPoint> ratioCutPointMap = new HashMap<>();

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

        ratioCutPointMap.put(key.get(), new RatioCutPoint(ratio, cutPointAvg));
    }

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException {
        float maxRatio = Float.MIN_VALUE;
        int selectedAttribute = -1;
        double selectedAttributeCutPoint = 0.0;

        for(Map.Entry<Integer, RatioCutPoint> ratioCutPointEntry : ratioCutPointMap.entrySet()) {
            if(ratioCutPointEntry.getValue().getRatio() > maxRatio) {
                selectedAttribute = ratioCutPointEntry.getKey();
                selectedAttributeCutPoint = ratioCutPointEntry.getValue().getCutPoint();
                maxRatio = ratioCutPointEntry.getValue().getRatio();
            }
        }
        IntWritable key = new IntWritable(selectedAttribute);
        DoubleWritable value1 = new DoubleWritable(selectedAttributeCutPoint);
        FloatWritable value2 = new FloatWritable(maxRatio);
        SelectMapperWritable selectMapperWritable = new SelectMapperWritable(value2, value1);

        long val = (long)(selectedAttributeCutPoint*100000);
        context.getCounter(SPLIT_COUNTER.CUTPOINT).increment(val);

        long selectedAttr = selectedAttribute;
        context.getCounter(SPLIT_COUNTER.ATTRIBUTE_COLUMN).increment(selectedAttr);
    }

}

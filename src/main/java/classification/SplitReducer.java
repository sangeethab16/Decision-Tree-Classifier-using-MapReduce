package classification;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SplitReducer extends Reducer<IntWritable, Text, Text, Text> {
    private final IntWritable result = new IntWritable();
    float minProbability = 1.0f;
    int maxRecordsInPartition = 1;

    @Override
    public void setup(Reducer.Context context) {
        minProbability = Float.parseFloat(context.getConfiguration().get("minProbability"));
        maxRecordsInPartition = Integer.parseInt(context.getConfiguration().get("maxRecordsInPartition"));
    }

    @Override
    public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        int countClassOne = 0;
        int countClassZero = 0;
        int total = 0;
        for (Text val : values) {
            int outputClass = Integer.parseInt(val.toString().split(",")[0]);
            if(outputClass == 0) {
                countClassZero++;
            }
            else {
                countClassOne++;
            }
            total+=1;
        }

        int maxCount = Math.max(countClassOne, countClassZero);

        int k = total == maxCount ? 1 : 2;

        if((maxCount/total) <= minProbability && total >= maxRecordsInPartition && k > 1) {
            for(Text val: values)
                context.write(new Text(""), val);
        }
        else {
            //add as leaf node
        }



//			context.write(key, new Text());
    }

}
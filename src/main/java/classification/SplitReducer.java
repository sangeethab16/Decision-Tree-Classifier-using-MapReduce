package classification;

import classification.utility.Node;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SplitReducer extends Reducer<IntWritable, Text, Text, Text> {
    private final IntWritable result = new IntWritable();
    float minProbability = 1.0f;
    int maxRecordsInPartition = 1;
    int ak;
    double cpk;
    private MultipleOutputs mos;

    @Override
    public void setup(Context context) {
        minProbability = Float.parseFloat(context.getConfiguration().get("minProbability"));
        maxRecordsInPartition = Integer.parseInt(context.getConfiguration().get("maxRecordsInPartition"));
        ak = Integer.parseInt(context.getConfiguration().get("selectedAttributeCutPoint"));
        cpk = Double.parseDouble(context.getConfiguration().get("selectedAttribute"));
        mos = new MultipleOutputs(context);
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

        int childId = -1;


        if(key.get() == 1) {
            //need to change key to parentKey which is read at mapper level
            childId = newId(key.get(), "left");
        }
        else {
            //need to change key to parentKey which is read at mapper level
            childId = newId(key.get(), "right");
        }

        if((maxCount/total) <= minProbability && total >= maxRecordsInPartition && k > 1) {
            //need to change key to parentKey which is read at mapper level
            mos.write( "text", key, new Text(ak + "" + cpk + "" + newId(key.get(), "left") + "," + newId(key.get(), "right")));
            for(Text val: values)
                context.write(new Text(""), new Text(val.toString() +"," + childId));
        }
        else {
            //need to change key to parentKey which is read at mapper level
            int maxCountClass = countClassOne > countClassZero? 1: 0;
            mos.write("text", key, new Text(maxCountClass + ""));
        }
    }

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException {

    }


    private int newId(int parentId, String childName) {
        if(childName.equals("left")) {
            return (parentId * 10) + 1;
        }
        else {
            return (parentId * 10) + 2;
        }
    }

}
package classification;

import classification.utility.Node;
import classification.utility.SPLIT_COUNTER;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitReducer extends Reducer<LongWritable, Text, Text, Text> {
    private final IntWritable result = new IntWritable();
    float minProbability = 1.0f;
    int maxRecordsInPartition = 1;
    int ak;
    double cpk;
    private MultipleOutputs mos;
    int parentKey;
    boolean isLeftLeafNode;
    boolean isRightLeafNode;
    int leftMaxCountClass;
    int rightMaxCountClass;
    Map<Integer, Node> parentChildren;

    @Override
    public void setup(Context context) {
        minProbability = Float.parseFloat(context.getConfiguration().get("minProbability"));
        maxRecordsInPartition = Integer.parseInt(context.getConfiguration().get("maxRecordsInPartition"));
        ak = Integer.parseInt(context.getConfiguration().get("selectedAttribute"));
        cpk = Double.parseDouble(context.getConfiguration().get("selectedAttributeCutPoint"));
        mos = new MultipleOutputs(context);
        parentKey = -1;
        isLeftLeafNode = false;
        isRightLeafNode = false;
        leftMaxCountClass = -1;
        rightMaxCountClass = -1;
        System.out.println("Just to check");
        parentChildren = new HashMap<>();
    }

    @Override
    public void reduce(final LongWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        int countClassOne = 0;
        int countClassZero = 0;
        int total = 0;

        long childId = -1;
        MarkableIterator<Text> mitr = new MarkableIterator<Text>(values.iterator());
        mitr.mark();

        if(key.get() == 1) {
            childId = newId(parentKey, "left");
        }
        else {
            //need to change key to parentKey which is read at mapper level
            childId = newId(parentKey, "right");
        }

        while (mitr.hasNext()) {
            Text temp = mitr.next();

            if(parentKey == -1) {
                String splitVal[] = temp.toString().split(",");
                try {
                    parentKey = Integer.parseInt(splitVal[splitVal.length-1]);
                }
                catch (Exception e) {
                    parentKey = 1;
                }
            }

            Double outputClass = Double.parseDouble(temp.toString().split(",")[0]);
            if(outputClass == 0.0) {
                countClassZero++;
            }
            else {
                countClassOne++;
            }
            total+=1;
        }

        int maxCount = Math.max(countClassOne, countClassZero);

        int k = total == maxCount ? 1 : 2;




        if((maxCount/((float)total)) <= minProbability && total >= maxRecordsInPartition && k > 1) {
        mitr.reset();
            context.getCounter(SPLIT_COUNTER.PARTITIONS).increment(1);
            while (mitr.hasNext()) {
                mos.write(NullWritable.get(), new Text(mitr.next().toString() +"," + childId), "data/partition" + key);
            }
        }
        else {
            System.out.println("here definitely");
            System.out.println("parm" + minProbability);
            System.out.println("minProb" + (maxCount/((float)total)));
            System.out.println("maxRecord" + total);
            System.out.println("k"+ k);

            if(key.get() == 1) {
                isLeftLeafNode = true;
                leftMaxCountClass = countClassOne > countClassZero? 1: 0;
            }
            else {
                isRightLeafNode = true;
                rightMaxCountClass = countClassOne > countClassZero? 1: 0;
            }
        }
    }


    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException {
        StringBuilder res = new StringBuilder();
        res.append(ak + "," + cpk + ",");
        if(isLeftLeafNode) {
            res.append("true" + "," + leftMaxCountClass + ",");
        }
        else {
            res.append("false" + "," + newId(parentKey, "left") + ",");
        }

        if(isRightLeafNode) {
            res.append("true" + "," + rightMaxCountClass);
        }
        else {
            res.append("false" + "," + newId(parentKey, "right"));
        }

        mos.write(new LongWritable(parentKey), new Text(res.toString()), "decision/tree");
        mos.close();
    }


    private long newId(long parentId, String childName) {
        if(parentId == 1) {
            if(childName.equals("left"))
                return 11;
            else
                return 12;
        }

        if(childName.equals("left")) {
            return (parentId * 10) + 1;
        }
        else {
            return (parentId * 10) + 2;
        }
    }

}
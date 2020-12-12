package classification;

import classification.utility.Node;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    }

    @Override
    public void reduce(final LongWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        int countClassOne = 0;
        int countClassZero = 0;
        int total = 0;

        long childId = -1;


        if(key.get() == 1) {
            childId = newId(key.get(), "left");
        }
        else {
            //need to change key to parentKey which is read at mapper level
            childId = newId(key.get(), "right");
        }

        System.out.println("here once");

        for (Text val : values) {
            Double outputClass = Double.parseDouble(val.toString().split(",")[0]);
            if(outputClass == 0.0) {
                countClassZero++;
            }
            else {
                countClassOne++;
            }
            total+=1;
            //mos.write("data", new Text(""), new Text(val.toString() +"," + childId));
            mos.write(new Text(""), new Text(val.toString() +"," + childId), "/" + key.get() + "datatest");
        }

        System.out.println("countClassZero" + countClassZero);
        System.out.println("countClassOne" + countClassOne);

        int maxCount = Math.max(countClassOne, countClassZero);

        int k = total == maxCount ? 1 : 2;




        System.out.println("minProb" + (maxCount/total));
        System.out.println("maxRecord" + total);
        System.out.println("k"+ k);

        if((maxCount/((float)total)) <= minProbability && total >= maxRecordsInPartition && k > 1) {
            //need to change key to parentKey which is read at mapper level

//            mos.write("data", new Text("tst"), new Text("nn"));
//            for(Text val: values) {
//                System.out.println("Most imp");
//                mos.write("data", new Text(""), new Text(val.toString() +"," + childId));
//            }
            System.out.println("Already written");
        }
        else {
            //delete the file created
            

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

        mos.write("tree", new LongWritable(parentKey), new Text(res.toString()));
        mos.close();
    }


    private long newId(long parentId, String childName) {
        if(parentId == -1) {
            if(childName.equals("left"))
                return 1;
            else
                return 2;
        }

        if(childName.equals("left")) {
            return (parentId * 10) + 1;
        }
        else {
            return (parentId * 10) + 2;
        }
    }

}
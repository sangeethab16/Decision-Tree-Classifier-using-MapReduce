package classification;

import classification.utility.SPLIT_COUNTER;
import classification.utility.SelectedAttribute;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitReducer extends Reducer<Text, Text, Text, Text> {
    private final IntWritable result = new IntWritable();
    float minProbability = 1.0f;
    int maxRecordsInPartition = 1;
    private MultipleOutputs mos;
    int parentKey;
//    boolean isLeftLeafNode;
//    boolean isRightLeafNode;
    int leftMaxCountClass;
    int rightMaxCountClass;
    Map<Integer, Node> parentChildren;
    Map<Integer, SelectedAttribute> parentSelectionAttr;

    @Override
    public void setup(Context context) throws IOException {
        minProbability = Float.parseFloat(context.getConfiguration().get("minProbability"));
        maxRecordsInPartition = Integer.parseInt(context.getConfiguration().get("maxRecordsInPartition"));
        mos = new MultipleOutputs(context);
        parentKey = -1;
//        isLeftLeafNode = false;
//        isRightLeafNode = false;
        leftMaxCountClass = -1;
        rightMaxCountClass = -1;
        System.out.println("Just to check");
        parentChildren = new HashMap<>();

        //add
        parentSelectionAttr = new HashMap<>();
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new RuntimeException(
                    "User information is not set in DistributedCache");
        }
        try
        {
            BufferedReader rdr = new BufferedReader(new FileReader("filelabel"));
            SelectedAttribute selectedAttribute;
            String line;
            // For each record in the user file
            while ((line = rdr.readLine()) != null) {
                String[] temp = line.split(",");
                selectedAttribute = new SelectedAttribute(Integer.parseInt(temp[2]),
                        Float.parseFloat(temp[1]));
                parentSelectionAttr.put(Integer.parseInt(temp[0]), selectedAttribute);
            }

        } catch (IOException e) {
            System.out.println("Some IO issue");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        int countClassOne = 0;
        int countClassZero = 0;
        int total = 0;

        int childId = -1;
        MarkableIterator<Text> mitr = new MarkableIterator<Text>(values.iterator());
        mitr.mark();

        parentKey = Integer.parseInt(key.toString().split(",")[0]);

        Node parentNode = null;

        if(parentChildren.get(parentKey) != null) {
            parentNode = parentChildren.get(parentKey);
        }
        else {
            parentNode = new Node();
            parentNode.setId(parentKey);
        }

        parentNode.setAttribute(parentSelectionAttr.get(parentKey).getAk());
        parentNode.setCutPoint(parentSelectionAttr.get(parentKey).getCpk());

        while (mitr.hasNext()) {
            Text temp = mitr.next();
            Double outputClass = Double.parseDouble(temp.toString().split(",")[0]);
            if(outputClass == 0.0) {
                countClassZero++;
            }
            else {
                countClassOne++;
            }
            total+=1;
        }

        if(key.toString().split(",")[1] == "1") {
            childId = newId(parentKey, "left");
            parentNode.setLeftChild(childId);
        }
        else {
            childId = newId(parentKey, "right");
            parentNode.setRightChild(childId);
        }

        int maxCount = Math.max(countClassOne, countClassZero);

        int k = total == maxCount ? 1 : 2;


        if((maxCount/((float)total)) <= minProbability && total >= maxRecordsInPartition && k > 1) {
        mitr.reset();

            while (mitr.hasNext()) {
                String val = mitr.next().toString();
                if(parentKey!=1) {
                    val = val.substring(0, val.lastIndexOf(","));
                }
                mos.write(NullWritable.get(), new Text(val +"," + childId), "data/partition" + key);
            }
        }
        else {
            if(key.toString().split(",")[1] == "1") {
                parentNode.setLeftFlag("true");
                leftMaxCountClass = countClassOne > countClassZero? 1: 0;
                parentNode.setLeftChild(leftMaxCountClass);
            }
            else {
                parentNode.setRightFlag("true");
                rightMaxCountClass = countClassOne > countClassZero? 1: 0;
                parentNode.setRightChild(rightMaxCountClass);
            }
        }
    }


    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException {
        StringBuilder res;
        for (Map.Entry<Integer, Node> entry : parentChildren.entrySet()) {

        res = new StringBuilder();
        res.append(entry.getValue().getAttribute() + "," + entry.getValue().getCutPoint() + ",");
        res.append(entry.getValue().getLeftFlag() + "," + entry.getValue().getLeftChild() + ",");
        res.append(entry.getValue().getRightFlag() + "," + entry.getValue().getRightChild());

        mos.write(new LongWritable(entry.getKey()), new Text(res.toString()), "decision/tree" + parentKey);

    }
        mos.close();
    }


    private int newId(int parentId, String childName) {
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
package classification;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SplitMapper extends Mapper<Object, Text, LongWritable, Text> {
    private final IntWritable result = new IntWritable();
    int ak;
    double cpk;

    @Override
    public void setup(Context context) {
        ak = Integer.parseInt(context.getConfiguration().get("selectedAttribute"));
        cpk = Double.parseDouble(context.getConfiguration().get("selectedAttributeCutPoint"));
    }
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

        if(value.toString().contains("label")) {
            return;
        }

        String[] row = value.toString().split(",");
        float[] rowValues = new float[row.length];
        for(int i=0;i< row.length; i++)
            rowValues[i] = Float.parseFloat(row[i]);
        int id = 0;
        if(rowValues[ak] > cpk)
            id = 1;
        else
            id = 0;
        context.write(new LongWritable(id),value);
    }

}
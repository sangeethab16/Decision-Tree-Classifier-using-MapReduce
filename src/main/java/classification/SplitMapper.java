package classification;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SplitMapper extends Mapper<Object, Text, Text, Text> {
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
        int parentKey = -1;
        if(value.toString().contains("label")) {
            return;
        }

        String[] row = value.toString().split(",");
        float rowValueAk = Float.parseFloat(row[ak]);

        try {
            parentKey = Integer.parseInt(row[row.length-1]);
        }
        catch (Exception e) {
            parentKey = 1;
        }

        int id = 0;
        if(rowValueAk > cpk) {
            id = 1;
        }
        else {
            id = 0;
        }
        context.write(new Text(parentKey + "," + id),new Text(value.toString()+"," + ak + "," + cpk));
    }

}
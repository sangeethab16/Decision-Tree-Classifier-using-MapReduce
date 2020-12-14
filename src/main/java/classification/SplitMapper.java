package classification;

import classification.utility.SelectedAttribute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SplitMapper extends Mapper<Object, Text, Text, Text> {
    private final IntWritable result = new IntWritable();
    Map<Integer, SelectedAttribute> parentSelectionAttr;

    @Override
    public void setup(Context context) throws IOException {
        //add
        int total = Integer.parseInt(context.getConfiguration().get("FilesTotal"));

        parentSelectionAttr = new HashMap<>();
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new RuntimeException(
                    "User information is not set in DistributedCache");
        }
        BufferedReader rdr = null;
        try
        {

            for(int i=0; i < total; i++) {
                rdr = new BufferedReader(new FileReader("filelabel" + i));
                SelectedAttribute selectedAttribute;
                String line;
                // For each record in the user file
                while ((line = rdr.readLine()) != null) {
                    String[] temp = line.split(",");
                    selectedAttribute = new SelectedAttribute(Integer.parseInt(temp[2]),
                            Float.parseFloat(temp[1]));
                    parentSelectionAttr.put(Integer.parseInt(temp[0]), selectedAttribute);
                }
            }


        } catch (IOException e) {

            System.out.println("Some IO issue");
            throw new RuntimeException(e);
        }
        finally {
            rdr.close();
        }
    }




    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        int parentKey = -1;
        if(value.toString().contains("label")) {
            return;
        }

        String[] row = value.toString().split(",");


        try {
            parentKey = Integer.parseInt(row[row.length-1]);
            System.out.println("parentKey" + parentKey);
        }
        catch (Exception e) {
            System.out.println("Someee error");
            parentKey = 1;
        }


        System.out.println("parentKey" + parentKey);

        int ak = parentSelectionAttr.get(parentKey).getAk();

        float rowAkVal = Float.parseFloat(row[ak]);

        float cpk = parentSelectionAttr.get(parentKey).getCpk();

        int id = 0;
        System.out.println("rowAkVal" + rowAkVal);
        System.out.println("cpk" + cpk);
        if(rowAkVal > cpk) {
            System.out.println("going here");
            id = 1;
        }
        else {
            System.out.println("going here too");
            id = 0;
        }
        context.write(new Text(parentKey + "," + id),new Text(value.toString()));
    }

}
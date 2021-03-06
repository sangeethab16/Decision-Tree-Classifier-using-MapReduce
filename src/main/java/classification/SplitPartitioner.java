package classification;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class SplitPartitioner extends Partitioner<Text, SelectMapperWritable> {

    @Override
    public int getPartition(Text key, SelectMapperWritable value, int numPartitions) {
        return Integer.parseInt(key.toString().split(",")[0]) % numPartitions;
    }

}

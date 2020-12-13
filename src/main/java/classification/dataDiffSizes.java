//obtain random subsets of the data of different sizes for running experiments with on AWS

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataDiffSizes extends Mapper<Object, Text, NullWritable, Text>{

}

package classification;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import classification.utility.SelectedAttribute;

public class PredictionMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private static final Logger logger = LogManager.getLogger(PredictionMapper.class);

	Map<Integer, Node> decisionTree = new HashMap<Integer, Node>();
	private static final String FILE_LABEL = "Tree";
	private static final Integer ONE = 1;
	MultipleOutputs multipleOutputs;

	@Override
	public void setup(Context context) throws IOException {

		multipleOutputs = new MultipleOutputs(context);
		int total = Integer.parseInt(context.getConfiguration().get("FilesTotal"));
		BufferedReader rdr = null;
		// Checking DistributedCache for files
		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles == null || !(cacheFiles.length > 0)) {

			throw new FileNotFoundException("Model file is not given to DistributedCache");
		}
		// Reading files from the DistributedCache

		try {

			for (int i = 0; i < total; i++) {
				rdr = new BufferedReader(new FileReader(FILE_LABEL + i));
				String rule;
				// For each record in the user file
				while ((rule = rdr.readLine()) != null) {
					System.out.println(rule);
					String[] nodeAttr = rule.split(",");
					decisionTree.put(Integer.parseInt(nodeAttr[0]), new Node(rule.toString()));
				}
			}

		} catch (IOException e) {

			System.out.println("Some IO issue");
			throw new RuntimeException(e);
		} finally {
			if (rdr != null) {
				rdr.close();
			}

		}
	}

	@Override
	public void map(final LongWritable key, final Text instance, final Context context)
			throws IOException, InterruptedException {

		if (key.get() == 0 && instance.toString().contains("label")) {
			return;
		}

		String[] instanceValues = instance.toString().split(",");

		Double predictedValue = Double.parseDouble(Helper.makePrediction(instance.toString(), decisionTree, ONE));

		multipleOutputs.write("PREDICTION", predictedValue.toString(), instance, "Prediction");

		if (predictedValue == Double.parseDouble(instanceValues[0])) {
			context.write(new IntWritable(1), new IntWritable(1));
		} else {
			context.write(new IntWritable(0), new IntWritable(1));
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}

}

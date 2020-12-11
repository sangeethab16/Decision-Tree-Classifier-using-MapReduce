package classification;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PredictionMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	private static final Logger logger = LogManager.getLogger(PredictionMapper.class);
	
	Map<Integer, Node> decisionTree = new HashMap<Integer, Node>();
	private static final String FILE_LABEL = "Tree";
	private static final Integer ONE = 1;

	@Override
	public void setup(Context context) throws IOException {
		
		BufferedReader reader = null;
		try {
			//Checking DistributedCache for files
			URI[] cacheFiles = context.getCacheFiles();
			System.out.println(cacheFiles.toString());

			if(cacheFiles == null || !(cacheFiles.length > 0)) {

				throw new FileNotFoundException("Model file is not given to DistributedCache");
			}

			//Reading files from the DistributedCache

			reader = new BufferedReader(new FileReader("Tree"));

			String rule;

			// For each record in the edge file
			while ((rule = reader.readLine()) != null) {

				String[] nodeAttr = rule.toString().split(",");
				decisionTree.put(Integer.parseInt(nodeAttr[0]), new Node(rule.toString()));
				
			}


			
		}catch (Exception e) {
			logger.info("Error occured while creating cache: " + e.getMessage());
			e.printStackTrace();
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}

	}

	@Override
	public void map(final LongWritable key, final Text instance, final Context context) throws IOException, InterruptedException {
		
		if(key.get() == 0) {
			return;
		}

		String[] instanceValues = instance.toString().split(",");
		
		String predictedValue = Helper.makePrediction(instance.toString(), decisionTree, ONE);
		
		String value = predictedValue + "," + instanceValues[0];
		
		context.write(new LongWritable(key.get()), new Text(value));
		

	}
	



}

package classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AttributeSelectionMapper extends Mapper<LongWritable, Text, Text, Text> {
	Map<Integer, List<Double>> attributeMetrics ;
	
	@Override
	public void setup(Context context) {
		
		//Initialize HashMap
		attributeMetrics = new HashMap<Integer, List<Double>>();
		
	}
	
	@Override
	public void map(final LongWritable key, final Text instance, final Context context) throws IOException, InterruptedException{
		
		if(key.get() == 0) {
			return;
		}
		
		String[] instanceValues = instance.toString().split(",");
		
		for (int i = 1; i < instanceValues.length; i++) {
			Double attrVal = Double.parseDouble(instanceValues[i]);
			if(attributeMetrics.containsKey(i)) {
				if(attributeMetrics.get(i) == null) {
					List<Double> attributeValues = new ArrayList<Double>();
					attributeValues.add(attrVal);
					attributeMetrics.put(i, attributeValues);
				} else {
					attributeMetrics.get(i).add(attrVal);
				}
			} else {
				List<Double> attributeValues = new ArrayList<Double>();
				attributeValues.add(attrVal);
				attributeMetrics.put(i, attributeValues);
			}
		}
		
	}
	
	@Override
	public void cleanup(Context context) {
		for(Map.Entry<Integer, List<Double>> entry : attributeMetrics.entrySet()) {
			//System.out.println("Attribute ID : " + entry.getKey());
			//System.out.println("Attribute Value List : " + entry.getValue().toString());
			List<Double> attrValList = entry.getValue();
			Collections.sort(attrValList);
			List<Double> cpList = new ArrayList<Double>();
			
			for(int i = 0; i < attrValList.size()-1; i++) {
				cpList.add(Double.sum(attrValList.get(i), attrValList.get(i+1))/2);
			}
			
			System.out.println("CUT POINT : " + entry.getKey() + " " + cpList.toString());
			
			
			
			
		}
			
		
	}
}
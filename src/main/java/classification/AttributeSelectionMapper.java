package classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AttributeSelectionMapper extends Mapper<LongWritable, Text, Text, Text> {
	Map<Integer, List<InstanceValue>> attributeMetrics ;
	Float classInfo;
	
	@Override
	public void setup(Context context) {
		
		//Initialize HashMap
		attributeMetrics = new HashMap<Integer, List<InstanceValue>>();
		classInfo = (float) 0.0;
		
		
	}
	
	@Override
	public void map(final LongWritable key, final Text instance, final Context context) throws IOException, InterruptedException{
		
		if(key.get() == 0) {
			return;
		}
		
		String[] instanceValues = instance.toString().split(",");
		
		for (int i = 0; i < instanceValues.length; i++) {
			Double attrVal = Double.parseDouble(instanceValues[i]);
			if(attributeMetrics.containsKey(i)) {
				if(attributeMetrics.get(i) == null) {
					List<InstanceValue> attributeValues = new ArrayList<InstanceValue>();
					attributeValues.add(new InstanceValue(attrVal,instanceValues[0]));
					attributeMetrics.put(i, attributeValues);
				} else {
					attributeMetrics.get(i).add(new InstanceValue(attrVal,instanceValues[0]));
				}
			} else {
				List<InstanceValue> attributeValues = new ArrayList<InstanceValue>();
				attributeValues.add(new InstanceValue(attrVal,instanceValues[0]));
				attributeMetrics.put(i, attributeValues);
			}
		}
		
		
	}
	
	@Override
	public void cleanup(Context context) {
		
		Float infoDataset = (float) 0.0;
		
		List<InstanceValue> classLabelValues = attributeMetrics.get(0);
		List<String> classLabels = new ArrayList<String>();
		
		for(int i = 0; i < classLabelValues.size(); i++) {
			
			classLabels.add(classLabelValues.get(i).getClassLabel());
		}
		
		Set<String> uniqueClassLabels = new HashSet<String>(classLabels);
		Map<String, Float> classProbability = new HashMap<String, Float>();
		
		for (Iterator<String> iter = uniqueClassLabels.iterator(); iter.hasNext(); ) {
			classProbability.put(iter.next(), (float)0.0);
	    }
		
		
		
		for(int i = 0; i < classLabels.size(); i++) {
			Float count = classProbability.get(classLabels.get(i));
			count++;
			classProbability.put(classLabels.get(i), count);
		}
		
		for (Iterator<String> iter = uniqueClassLabels.iterator(); iter.hasNext(); ) {
			String classLabel = iter.next();
			classProbability.put(classLabel, classProbability.get(classLabel)/classLabels.size());
	    }
		
		//System.out.println("CLASS PROBABILITY : " + classProbability.toString());
		
		
		
		for(Map.Entry<Integer, List<InstanceValue>> entry : attributeMetrics.entrySet()) {
			//System.out.println("Attribute ID : " + entry.getKey());
			//System.out.println("Attribute Value List : " + entry.getValue().toString());
			List<InstanceValue> attrValList = entry.getValue();
			Collections.sort(attrValList, new SortByValue());
			List<Double> cpList = new ArrayList<Double>();
			
			for(int i = 0; i < attrValList.size()-1; i++) {
				cpList.add(Double.sum(attrValList.get(i).getValue(), attrValList.get(i+1).getValue())/2);
			}
			
			//System.out.println("CUT POINT : " + entry.getKey() + " " + cpList.toString());
			/*for(int i = 0; i< cpList.size(); i++) {
				
			}*/
			
		}
		
		
			
		
	}
}
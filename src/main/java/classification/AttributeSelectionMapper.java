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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AttributeSelectionMapper extends Mapper<LongWritable, Text, IntWritable, SelectMapperWritable> {
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
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		List<String> classLabels = new ArrayList<String>();
		
		for(int i = 0; i < attributeMetrics.get(0).size(); i++) {
			
			classLabels.add(attributeMetrics.get(0).get(i).getClassLabel());
		}
		
		Set<String> uniqueClassLabels = new HashSet<String>(classLabels);
		
		
		Float infoDataset = Helper.infoCalc(uniqueClassLabels, attributeMetrics.get(0));
		
		System.out.println("INFO : " + infoDataset);
		
		
		for(Map.Entry<Integer, List<InstanceValue>> entry : attributeMetrics.entrySet()) {
			
			if (entry.getKey() == 0) {
				continue;
			}
			List<InstanceValue> attrValList = entry.getValue();
			Collections.sort(attrValList, new SortByValue());
			List<Double> cpList = new ArrayList<Double>();
			
			for(int i = 0; i < attrValList.size()-1; i++) {
				cpList.add(Double.sum(attrValList.get(i).getValue(), attrValList.get(i+1).getValue())/2);
			}
			
			Float maxGain = (float) 0.0;
			Double maxGainCP = 0.0;
			float split;
			float ratio = 0;
			
			
			for(int i = 0; i< cpList.size(); i++) {
				
				List<InstanceValue> attrValLessCP = new ArrayList<InstanceValue>();
				List<InstanceValue> attrValGreaterCP = new ArrayList<InstanceValue>();
				
				for (Iterator<InstanceValue> iter = attrValList.iterator(); iter.hasNext(); ) {
					InstanceValue attrVal = iter.next();
					SortByValue comp = new SortByValue();
					
					if(comp.compareToVal(attrVal, cpList.get(i)) <= 0) {
						
						attrValLessCP.add(attrVal);
						
					} else {
						
						attrValGreaterCP.add(attrVal);
						
					}
					
					
				}
				
				float Xij1 = ((float)attrValLessCP.size())/attrValList.size();
				float Xij2 = ((float)attrValGreaterCP.size())/attrValList.size();
				
				Float gain = infoDataset - (Xij1 * Helper.infoCalc(uniqueClassLabels, attrValLessCP) + 
						Xij2 * Helper.infoCalc(uniqueClassLabels, attrValGreaterCP));
				if (gain > maxGain) {
					maxGain = gain;
					maxGainCP = cpList.get(i);
					float prop1 = ((float)attrValLessCP.size()/attrValList.size());
					float prop2 = ((float)attrValGreaterCP.size()/attrValList.size());
					split = (prop1 * (float) (Math.log(prop1) / Math.log(2))) + (prop2 * (float) (Math.log(prop2) / Math.log(2)));
					split = (float) (split * -1.0);
					ratio = gain / split;
				}
				
				
			}
			context.write(new IntWritable(entry.getKey()), new SelectMapperWritable(new FloatWritable(ratio),new DoubleWritable(maxGainCP)));

			
			
			
		}	
		
	}
}


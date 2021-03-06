package classification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Helper {
	
	public static Float infoCalc(Set<String> uniqueClassLabels, List<InstanceValue> instanceValues) {

		List<String> classLabels = new ArrayList<String>();
		
		for(int i = 0; i < instanceValues.size(); i++) {
			
			classLabels.add(instanceValues.get(i).getClassLabel());
		}
		
		Map<String, Integer> classCount = new HashMap<String, Integer>();
		
		for (Iterator<String> iter = uniqueClassLabels.iterator(); iter.hasNext(); ) {
			classCount.put(iter.next(), 0);
	    }
		
		
		
		for(int i = 0; i < classLabels.size(); i++) {
			Integer count = classCount.get(classLabels.get(i));
			count++;
			classCount.put(classLabels.get(i), count);
		}
		
		Float info = (float) 0.0;
		
		for (Iterator<String> iter = uniqueClassLabels.iterator(); iter.hasNext(); ) {
			String classLabel = iter.next();
			
			Float prob =  ((float)classCount.get(classLabel)/classLabels.size());
			
			if(prob == (float)0.0) {
				return (float) 0.0;
			}
			
			info = Float.sum(info, prob*(float) (Math.log(prob) / Math.log(2)));
			
			
	    }
		
		info = info * -1;
		return info;
	}
	
	public static String makePrediction(String instance, Map<Integer, Node> decisionTree, Integer rootID) {
		
		String[] instanceValues = instance.split(",");
		Node rootNode = decisionTree.get(rootID);
		String predictedValue = null;
		
		
		if (Float.parseFloat(instanceValues[rootNode.getAttribute()]) <= rootNode.getCutPoint()) {
			
			if (rootNode.getLeftFlag().equals("false")) {
				
				
				predictedValue = makePrediction(instance, decisionTree, rootNode.getLeftChild());
			}
			
			else {
				predictedValue = rootNode.getLeftChild().toString();
			}
		}
		
		else {
			
			if (rootNode.getRightFlag().equals("false")) {
				
				predictedValue = makePrediction(instance, decisionTree, rootNode.getRightChild());
			}
			
			else {
				predictedValue = rootNode.getRightChild().toString();
			}
		}
		
		return predictedValue;
		
	}

}

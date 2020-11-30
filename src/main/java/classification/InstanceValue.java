package classification;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public class InstanceValue {

    private Double value;
    private String classLabel;
    
    public InstanceValue(Double value, String classLabel) {
    	this.value = value;
    	this.classLabel = classLabel;
    }
    
    public void setValue(Double value) {
    	this.value = value;
    }
    
    public void setClassLabel(String classLabel) {
    	this.classLabel = classLabel;
    }
    
    public Double getValue() {
    	return this.value;
    }
    
    public String getClassLabel() {
    	return this.classLabel;
    }


    @Override
    public String toString() {
        return this.value + "," + this.classLabel;
    }
}
class SortByValue implements Comparator<InstanceValue> 
{ 
    // Used for sorting in ascending order of 
    // attribute value


	@Override
	public int compare(InstanceValue o1, InstanceValue o2) {
		
		if (o1.getValue() < o2.getValue()) return -1;
        if (o1.getValue() > o2.getValue()) return 1;
        return 0;
	} 
	
	public int compareToVal(InstanceValue o1, Double val) {
		
		if (o1.getValue() < val) return -1;
        if (o1.getValue() > val) return 1;
        return 0;
	} 
	
	
} 

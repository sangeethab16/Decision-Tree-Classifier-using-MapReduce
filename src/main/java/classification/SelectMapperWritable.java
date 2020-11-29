package classification;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SelectMapperWritable implements Writable {

    private FloatWritable ratio;
    private DoubleWritable attributeValue;

    public SelectMapperWritable(FloatWritable ratio, DoubleWritable attributeValue) {
        this.ratio = ratio;
        this.attributeValue = attributeValue;
    }

    public SelectMapperWritable() {
        ratio = new FloatWritable();
        attributeValue = new DoubleWritable();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        ratio.write(out);
        attributeValue.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ratio.readFields(in);
        attributeValue.readFields(in);
    }

    public FloatWritable getRatio() {
        return ratio;
    }

    public void setRatio(FloatWritable ratio) {
        this.ratio = ratio;
    }

    public DoubleWritable getAttributeValue() {
        return attributeValue;
    }

    public void setAttributeValue(DoubleWritable attributeValue) {
        this.attributeValue = attributeValue;
    }


    @Override
    public String toString() {
        return ratio + "," + attributeValue;
    }
}

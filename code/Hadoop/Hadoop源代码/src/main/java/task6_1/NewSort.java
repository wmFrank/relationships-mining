package task6_1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NewSort implements WritableComparable<NewSort> {
    public double data;

    public NewSort(){
        data = 0.0;
    }

    public NewSort(double data){
        this.data = data;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(data);
    }

    public void readFields(DataInput in) throws IOException {
        this.data = in.readDouble();
    }

    public int compareTo(NewSort newsort){
        if(newsort.data - data > 0)
            return 1;
        else
            return -1;
    }

    public boolean equals(Object obj){
        return super.equals(obj);
    }

    public int hashCode(){
        return super.hashCode();
    }

    public String toString(){
        return Double.toString(data);
    }
}

package fiu.edu.cs.ds.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WritableNeightbor implements Writable{
	
	private int c;
	private float distance;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		c = in.readInt();
		distance = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(c);
		out.writeFloat(distance);
	}
	
	public static WritableNeightbor read(DataInput in) throws IOException {
		WritableNeightbor w = new WritableNeightbor();
        w.readFields(in);
        return w;
     }
	
	public static WritableNeightbor getWritableNeightborArray(int c, float d){
		WritableNeightbor out = new WritableNeightbor();
		out.c = c;
		out.distance = d;
		return out;
	}
	
	public float getDistance(){
		return distance;
	}
	
	public int getClassification(){
		return c;
	}

}

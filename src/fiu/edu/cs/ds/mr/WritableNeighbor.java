package fiu.edu.cs.ds.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WritableNeighbor implements Writable{
	
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
	
	public static WritableNeighbor read(DataInput in) throws IOException {
		WritableNeighbor w = new WritableNeighbor();
        w.readFields(in);
        return w;
     }
	
	public static WritableNeighbor getWritableNeighbor(int c, float d){
		WritableNeighbor out = new WritableNeighbor();
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

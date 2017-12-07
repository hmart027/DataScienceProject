package fiu.edu.cs.ds.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.io.Writable;

public class WritableFloatArray implements Writable{
	
	private float[] array;

	@Override
	public void readFields(DataInput in) throws IOException {
		for(int i=0; i<array.length; i++){
			array[i] = in.readFloat();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for(int i=0; i<array.length; i++){
			out.writeFloat(array[i]);
		}
	}
	
	public static WritableFloatArray read(DataInput in) throws IOException {
		WritableFloatArray w = new WritableFloatArray();
        w.readFields(in);
        return w;
     }
	
	public static WritableFloatArray getWritableFloatArray(float[] a) throws NullArgumentException{
		if(a==null) throw new NullArgumentException("Null array passed");
		WritableFloatArray out = new WritableFloatArray();
		out.array = a;
		return out;
	}

}

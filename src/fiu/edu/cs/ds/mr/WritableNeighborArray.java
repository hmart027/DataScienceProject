package fiu.edu.cs.ds.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class WritableNeighborArray  implements Writable{
	
	private int size = 0;
	private ArrayList<Integer> classes;
	private ArrayList<Float> distances;
	
	public WritableNeighborArray(){
		classes = new ArrayList<>();
		distances = new ArrayList<>();
	}
	
	public WritableNeighborArray(int c, float d){
		this();
		size = 1;
		classes.add(c);
		distances.add(d);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		for(int i=0; i<size; i++){
			classes.add(in.readInt());
		}
		for(int i=0; i<size; i++){
			distances.add(in.readFloat());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for(int i=0; i<size; i++){
			out.writeInt(classes.get(i));
		}
		for(int i=0; i<size; i++){
			out.writeFloat(distances.get(i));
		}
	}
	
	public static WritableNeighborArray read(DataInput in) throws IOException {
		WritableNeighborArray w = new WritableNeighborArray();
        w.readFields(in);
        return w;
    }
		
	public void addNeighbor(int c, float d){
		classes.add(c);
		distances.add(d);
		size +=1 ;
	}
	
	public void addNeighbors(WritableNeighborArray ns){
		if(ns==null) return;
		classes.addAll(ns.classes);
		distances.addAll(ns.distances);
		size+=ns.size;
	}

	public Integer getClassification(int index){
		if(index<0 || index>size) return null;
		return classes.get(index);
	}
	
	public Float getDistance(int index){
		if(index<0 || index>size) return null;
		return distances.get(index);
	}
	
	public int getSize(){
		return size;
	}
	
}

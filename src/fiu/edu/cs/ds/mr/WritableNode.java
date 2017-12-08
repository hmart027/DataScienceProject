package fiu.edu.cs.ds.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class WritableNode implements Writable{
	private int k = 1;
	private Float max = Float.MAX_VALUE;
	private TreeMap<Float, Integer> neighbors;
	
	public WritableNode(){
		k=1;
		neighbors = new TreeMap<>();
	}
	
	public WritableNode(int k){
		this();
		this.k = k;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		k = in.readInt();
		max = in.readFloat();
		for(int i=0; i<k; i++){
			Float k = in.readFloat();
			Integer v = in.readInt();
			neighbors.put(k, v);
		}
	}
	
	public static WritableNode read(DataInput in) throws IOException {
		WritableNode w = new WritableNode();
        w.readFields(in);
        return w;
    }
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(k);
		out.writeFloat(max);
		for(java.util.NavigableMap.Entry<Float,Integer> e: neighbors.entrySet()){
			out.writeFloat(e.getKey());
			out.writeInt(e.getValue());
		}
	}
	
	public void addNeighbor(int c, float d){
		// distance is the key here;
		if(d<max){
			neighbors.put(d, c);
			if (neighbors.size() > k) {
				neighbors.remove(neighbors.lastKey());
			}
			max = neighbors.lastKey();
		}
	}
	
	public void addNeighbors(WritableNode n){
		if(n==null || n.k != k)
			return;
		for(java.util.NavigableMap.Entry<Float,Integer> e: n.neighbors.entrySet()){
			addNeighbor(e.getValue(), e.getKey());
		}
	}
	
	public int getClassification(){
		// class is mapped to the number of times it appears
		TreeMap<Integer, Integer> count = new TreeMap<>();
		int max = 0;
		int key = 0;
		for (int cl : neighbors.values()) {
			Integer c = count.get(cl);
			if (c == null)
				c = 0;
			c++;
			count.put(cl, c);
			if(c>max){
				key = cl;
				max = c;
			}
		}
		return key;
	}

}

package fiu.edu.cs.ds.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class WritableNode implements Writable{
	private int k = 1;
	private float max = Float.MAX_VALUE;
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
		int s = in.readInt();
		max = in.readFloat();
		neighbors = new TreeMap<>();
		for(int i=0; i<s; i++){
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
		int s = neighbors.size();
		out.writeInt(k);
		out.writeInt(s);
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
			while (neighbors.size() > k) {
				neighbors.remove(neighbors.lastKey());
			}
			max = neighbors.lastKey();
		}else{
			if(neighbors.size() < k){
				neighbors.put(d, c);
				max = neighbors.lastKey();
			}
		}
	}
	
	public void addNeighbors(WritableNode n){
		if(n==null || n.k != k){
			return;
		}
		for(java.util.NavigableMap.Entry<Float,Integer> e: n.neighbors.entrySet()){
			addNeighbor(e.getValue(), e.getKey());
		}
	}
	
	public int getClassification(){

		//class is mapped to the number of times it appears
		TreeMap<Integer, Integer> count = new TreeMap<>();
		int max = 0;
		int key = 0;
		for(float d: neighbors.keySet()){
			Integer cl = neighbors.get(d);
			Integer c = count.get(cl);
			if(c==null)
				c = 0;
			c++;
			count.put(cl, c);
			if(c>max){
				key = cl;
				max = c;
			}
		}
		
//		// class is mapped to the number of times it appears
//		TreeMap<Integer, Float> counts = new TreeMap<>();
//		float minD = 0;
//		int key = 0;
//		for (float d: neighbors.keySet()) {
//			Integer cl = neighbors.get(d);
//			Float c = counts.get(cl);
//			if (c == null)
//				c = 0f;
//			c+=d;
//			counts.put(cl, c);			
//			
//		}
//		key = counts.firstKey();
//		minD = counts.get(key);
//		for(int c: counts.keySet()){
//			float d = counts.get(c);
//			if(c<minD){
//				key = c;
//				minD = d;
//			}
//		}
		
		return key;
	}
	
	public int getNeighborCount(){
		return neighbors.size();
	}
	
	public int getK(){
		return k;
	}

	@Override
	public WritableNode clone(){
		WritableNode n = new WritableNode();
		n.k=k;
		n.max=max;
		n.neighbors.putAll(neighbors);
		return n;
	}
}

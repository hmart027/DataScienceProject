package fiu.edu.cs.ds.mr;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNCombiner extends Reducer<IntWritable, WritableNeighborArray, IntWritable, WritableNeighborArray>{
	
	@Override
	public void reduce(IntWritable testSampleIndex, Iterable<WritableNeighborArray> neightbors, Context context)
			throws IOException, InterruptedException {
		
//		System.out.println("Combining Key: "+testSampleIndex.get());
		int k = context.getConfiguration().getInt("k", 1);
		// distance is the key here;
		TreeMap<Double, Integer> nn = new TreeMap<>();
		for (WritableNeighborArray nA : neightbors) {
			for(int i=0; i<nA.getSize(); i++){
				double d = nA.getDistance(i);
				nn.put(d, nA.getClassification(i));
				while (nn.size() > k) {
					nn.remove(nn.lastKey());
				}
			}
		}
		// class is mapped to the number of times it appears
		TreeMap<Integer, Integer> count = new TreeMap<>();
		int max = 0;
		int key = 0;
		for (int cl : nn.values()) {
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
		
		context.write(testSampleIndex, new WritableNeighborArray(key, max));
	}
}

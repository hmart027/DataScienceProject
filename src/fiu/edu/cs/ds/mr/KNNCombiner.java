package fiu.edu.cs.ds.mr;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNCombiner extends Reducer<IntWritable, WritableNeightbor, IntWritable, IntWritable>{
	
	@Override
	public void reduce(IntWritable testSampleIndex, Iterable<WritableNeightbor> neightbors, Context context)
			throws IOException, InterruptedException {
		int k = context.getConfiguration().getInt("k", 1);
		// distance is the key here;
		TreeMap<Double, Integer> nn = new TreeMap<>();
		for (WritableNeightbor n : neightbors) {
			double d = n.getDistance();
			nn.put(d, n.getClassification());
			while (nn.size() > k) {
				nn.remove(nn.lastKey());
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
		context.write(testSampleIndex, new IntWritable(key));
		
	}
}

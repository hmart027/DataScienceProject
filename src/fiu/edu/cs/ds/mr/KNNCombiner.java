package fiu.edu.cs.ds.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNCombiner extends Reducer<IntWritable, WritableNode, IntWritable, WritableNode>{
	
	@Override
	public void reduce(IntWritable testSampleIndex, Iterable<WritableNode> neighbors, Context context)
			throws IOException, InterruptedException {
	
		WritableNode tn = neighbors.iterator().next();
		for(WritableNode n : neighbors){
			tn.addNeighbors(n);
		}
		context.write(testSampleIndex, tn);
	}
}

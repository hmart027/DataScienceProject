package fiu.edu.cs.ds.mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

// Gets the index of the testDataset element and its closest neighbors. Returns the index of the testDataset element and its class
public class KNNReduce extends Reducer<IntWritable, WritableNode, IntWritable, IntWritable>{
	
	@Override
	public void reduce(IntWritable testSampleIndex, Iterable<WritableNode> neighbors, Context context)
			throws IOException, InterruptedException {
		WritableNode tn = neighbors.iterator().next().clone();
		for(WritableNode n : neighbors){
			tn.addNeighbors(n);
		}
		context.write(testSampleIndex, new IntWritable(tn.getClassification()));
		
	}

}

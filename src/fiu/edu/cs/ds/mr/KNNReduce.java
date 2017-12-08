package fiu.edu.cs.ds.mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

// Gets the index of the testDataset element and its closest neighbors. Returns the index of the testDataset element and its class
public class KNNReduce extends Reducer<IntWritable, WritableNode, IntWritable, IntWritable>{
	
	@Override
	public void reduce(IntWritable testSampleIndex, Iterable<WritableNode> neightbors, Context context)
			throws IOException, InterruptedException {
		
		WritableNode lN = neightbors.iterator().next();
		context.write(testSampleIndex, new IntWritable(lN.getClassification()));
		
	}

}

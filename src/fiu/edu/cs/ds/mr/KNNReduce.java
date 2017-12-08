package fiu.edu.cs.ds.mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

// Gets the index of the testDataset element and its closest neighbors. Returns the index of the testDataset element and its class
public class KNNReduce extends Reducer<IntWritable, WritableNeighborArray, IntWritable, IntWritable>{
	
	@Override
	public void reduce(IntWritable testSampleIndex, Iterable<WritableNeighborArray> neightbors, Context context)
			throws IOException, InterruptedException {
		
//		System.out.println("Reducing Key: "+testSampleIndex.get());
		context.write(testSampleIndex, new IntWritable(neightbors.iterator().next().getClassification(0)));
		
	}

}

package fiu.edu.cs.ds.mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

// Gets the index of the testDataset element and its closest neighbors. Returns the index of the testDataset element and its class
public class KNNReduce extends Reducer<IntWritable, WritableNeightbor, IntWritable, IntWritable>{

	@Override
	public void reduce(IntWritable testSampleIndex, Iterable<WritableNeightbor> classes, Context context)
			throws IOException, InterruptedException {
		context.write(testSampleIndex, new IntWritable(classes.iterator().next().getClassification()));
		
	}

}

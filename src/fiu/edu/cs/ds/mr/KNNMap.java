package fiu.edu.cs.ds.mr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KNNMap implements Mapper<LongWritable, Text, Text, IntWritable>{

	float[] tSample;
	
	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	//Pass the training data to this method
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		String[] seg = arg1.toString().split(" ");
		float[] s = new float[seg.length];
		for(int i=0; i<s.length; i++){
			s[i] = Float.parseFloat(seg[i]);
		}
		
		
	}

}

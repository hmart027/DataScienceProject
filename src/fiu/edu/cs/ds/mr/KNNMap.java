package fiu.edu.cs.ds.mr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMap extends Mapper<LongWritable, Text, IntWritable, WritableNeightbor>{

	int k = 1;
	ArrayList<float[]> tSamples;
	
//	@Override
//	public void setup(Context cxt) throws IOException {
//		
//	}
//
//	@Override
//	public void cleanup(Context cxt) throws IOException {
//		// TODO Auto-generated method stub
//		
//	}

	@Override
	//Pass the training data to this method
	public void map(LongWritable arg0, Text arg1, Context cxt)
			throws IOException, InterruptedException {
		String[] seg = arg1.toString().split(" ");
		float[] trainSample = new float[seg.length];
		for(int i=0; i<trainSample.length; i++){
			trainSample[i] = Float.parseFloat(seg[i]);
		}
		for(int i=0; i<tSamples.size(); i++){
			float[] tS = tSamples.get(i);
			cxt.write(new IntWritable(i), WritableNeightbor.getWritableNeightborArray((int)trainSample[0], (float)getDistance(trainSample, tS)));
		}
	}
	
	public double getDistance(float[] trainS, float[] testS){
		int l = testS.length;
		double d = 0;
		for(int i=1; i<l; i++){
			d += (testS[i]-trainS[i])*(testS[i]-trainS[i]);
		}
		return Math.sqrt(d);
	}

}

package fiu.edu.cs.ds.mr;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMap extends Mapper<LongWritable, Text, IntWritable, WritableNode>{

	ArrayList<float[]> tSamples;
	WritableNode[] res; 
	
	@Override
	public void setup(Context cxt) throws IOException {
		String path = cxt.getConfiguration().get("TEST_PATH");
		tSamples = KNNMapReduce.loadDataSet(path, cxt.getConfiguration());
		res = new WritableNode[tSamples.size()];
		int k = cxt.getConfiguration().getInt("k", 1);
		for(int i=0; i<res.length; i++){
			res[i] = new WritableNode(k);
		}
	}
	
	public void cleanup(Context cxt) throws IOException, InterruptedException{
		for(int i=0; i<res.length; i++){
			cxt.write(new IntWritable(i), res[i]);
		}
	}

	@Override
	//Pass the training data to this method
	public void map(LongWritable arg0, Text arg1, Context cxt)
			throws IOException, InterruptedException {
		String[] seg = arg1.toString().split(" ");
		float[] trainSample = new float[seg.length];
		for(int i=0; i<trainSample.length; i++){
			trainSample[i] = Float.parseFloat(seg[i]);
		}
		int c = (int)trainSample[0];
		for(int i=0; i<tSamples.size(); i++){
			float[] tS = tSamples.get(i);
			res[i].addNeighbor(c, (float)KNNMapReduce.getDistance(trainSample, tS));
		}
		
	}
	
}

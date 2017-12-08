package fiu.edu.cs.ds.mr;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMap extends Mapper<LongWritable, Text, IntWritable, WritableNeighborArray>{

	ArrayList<float[]> tSamples;
	
	@Override
	public void setup(Context cxt) throws IOException {
		String path = cxt.getConfiguration().get("TEST_PATH");
		tSamples = KNNMapReduce.loadDataSet(path, cxt.getConfiguration());
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
		for(int i=0; i<tSamples.size(); i++){
			float[] tS = tSamples.get(i);
			cxt.write(new IntWritable(i),
//					WritableNeighbor.getWritableNeighbor((int)trainSample[0], (float)getDistance(trainSample, tS)));
					new WritableNeighborArray((int)trainSample[0], (float)KNNMapReduce.getDistance(trainSample, tS)));
		}
	}
	
}

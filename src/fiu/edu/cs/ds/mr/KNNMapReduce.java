package fiu.edu.cs.ds.mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KNNMapReduce {
	
	public static void main(String[] args) throws IOException{
		
		System.out.println("Loading Data.....");
		ArrayList<float[]> trainSamples = loadDataSet("zip.train");
		ArrayList<float[]> testSamples = loadDataSet("zip.test");
		System.out.println("Training samples: "+trainSamples.size());
		System.out.println("Testing samples: "+testSamples.size());
		
		System.out.println("Starting Map Reduce.....");
		
		JobConf conf = new JobConf(KNNMapReduce.class);
		conf.setJobName("knn");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(KNNMap.class);
		conf.setCombinerClass(KNNReduce.class);
		conf.setReducerClass(KNNReduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
	
	public static ArrayList<float[]> loadDataSet(String path){
		ArrayList<float[]> data = new ArrayList<>();
		try {
			BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
			while(input.ready()){
				String[] seg = input.readLine().split(" ");
				float[] s = new float[seg.length];
				for(int i=0; i<s.length; i++){
					s[i] = Float.parseFloat(seg[i]);
				}
				data.add(s);
			}
			input.close();
			return data;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}

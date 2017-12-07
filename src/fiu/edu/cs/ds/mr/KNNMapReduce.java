package fiu.edu.cs.ds.mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNMapReduce {
	
	public static void main(String[] args) throws Exception{
		
		System.out.println("Loading Data.....");
		ArrayList<float[]> trainSamples = loadDataSet("zip.train");
		ArrayList<float[]> testSamples = loadDataSet("zip.test");
		System.out.println("Training samples: "+trainSamples.size());
		System.out.println("Testing samples: "+testSamples.size());
		
		System.out.println("Starting Map Reduce.....");
		
		Configuration conf = new Configuration();
		conf.setInt("k", 1);
		conf.set("TEST_PATH", "zip.test");
		
		Job job = Job.getInstance(conf, "knn");
		
		job.setJarByClass(KNNMapReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(KNNMap.class);
		job.setCombinerClass(KNNCombiner.class);
		job.setReducerClass(KNNReduce.class);
				
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
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

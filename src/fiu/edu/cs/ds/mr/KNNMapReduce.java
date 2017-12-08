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
		
		int mapperCount = 1;
		int reducerCount = 1;
		
		Configuration conf = new Configuration();
		
		System.out.println("Loading Data.....");
		ArrayList<float[]> trainSamples = loadDataSet(args[1], conf);
		ArrayList<float[]> testSamples = loadDataSet(args[2], conf);
		System.out.println("Training samples: "+trainSamples.size());
		System.out.println("Testing samples: "+testSamples.size());
		
		System.out.println("Starting Map Reduce.....");
		
		int k = 1;
		//Read number of NN to look at
		if(args.length > 4)
			k = Integer.parseInt(args[4]);

		//Read number of desired mappers
		if(args.length > 5)
			mapperCount = Integer.parseInt(args[5]);

		//Read number of reducers
		if(args.length > 6)
			reducerCount = Integer.parseInt(args[6]);
		
		conf.setInt("k", k);
		conf.set("TEST_PATH", args[2]);
		conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 14512253/mapperCount);
		
		Job job = Job.getInstance(conf, "knn");
		
		job.setJarByClass(KNNMapReduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(WritableNode.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(KNNMap.class);
		job.setCombinerClass(KNNCombiner.class);
		job.setReducerClass(KNNReduce.class);
		
		job.setNumReduceTasks(reducerCount);
				
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		long t0 = System.currentTimeMillis();
		boolean status = job.waitForCompletion(true);
		System.out.println("Ellapsed time: "+(float)(System.currentTimeMillis()-t0)/1000f);
		
		System.out.println("Error Rate: "+getErrorRate(testSamples, args[3], conf));
		
		System.exit(status? 0 : 1);
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
	
	public static ArrayList<float[]> loadDataSet(String path, Configuration cfg){
		ArrayList<float[]> data = new ArrayList<>();
		try {
			Path p = new Path(path);
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getFileSystem(cfg).open(p)));
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

	public static double getDistance(float[] trainS, float[] testS){
		int l = testS.length;
		double d = 0;
		for(int i=1; i<l; i++){
			d += (testS[i]-trainS[i])*(testS[i]-trainS[i]);
		}
		return Math.sqrt(d);
	}

	public static float getErrorRate(ArrayList<float[]> testSamples, String path, Configuration cfg){
		float eRate = 1f/(float)testSamples.size();
		int errorCount = 0;
		try {
			Path p = new Path(path+"/part-r-00000");
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getFileSystem(cfg).open(p)));
			while(input.ready()){
				String[] seg = input.readLine().split("\t");
				int i = Integer.parseInt(seg[0]);
				int c = Integer.parseInt(seg[1]);
				if(c != (int)testSamples.get(i)[0])
					errorCount++;
			}
			input.close();
			System.out.println(errorCount);
			return eRate*errorCount;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
}

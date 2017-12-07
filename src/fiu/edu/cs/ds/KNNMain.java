package fiu.edu.cs.ds;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class KNNMain {

	public static void main(String[] args) {
		ArrayList<float[]> trainSamples = loadDataSet("zip.train");
		ArrayList<float[]> testSamples = loadDataSet("zip.test");
		System.out.println("Training samples: "+trainSamples.size());
		System.out.println("Testing samples: "+testSamples.size());
		
		trainSamples = new ArrayList<>(trainSamples.subList(0, 1000));
		
		KNN knn = new KNN();
		knn.train(trainSamples);
		
		long t0 = 0;
		int eCount = 0;
		for(int k = 0; k<13; k++){
			eCount = 0;
			t0 = System.currentTimeMillis();
			for(float[] s: testSamples){
				float c = knn.classify(s, 2*k+1);
				if((int)c!=(int)s[0]){
					eCount++;
				}
			}
			System.out.println("Error Rate for k="+(2*k+1)+": "+((double)eCount/(double)testSamples.size()));
			System.out.println("Ellapsed time: "+(float)(System.currentTimeMillis()-t0)/1000f);
		}
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

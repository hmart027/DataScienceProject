package fiu.edu.cs.ds;

import java.util.ArrayList;
import java.util.TreeMap;

public class KNN {
	
	private ArrayList<float[]> trainingData;
	
	public void train(ArrayList<float[]> trainingData){
		this.trainingData = trainingData;
	}
	
	public double getDistance(float[] trainS, float[] testS){
		int l = testS.length;
		double d = 0;
		for(int i=1; i<l; i++){
			d += (testS[i]-trainS[i])*(testS[i]-trainS[i]);
		}
		return Math.sqrt(d);
	}
	
	public float classify(float[] testS, int kN){
		//distance is the key here;
		TreeMap<Double, float[]> nn = new TreeMap<>();
		for(float[] trainS: trainingData){
			double d = getDistance(trainS, testS);
			nn.put(d, trainS);
			while(nn.size()>kN){
				nn.remove(nn.lastKey());
			}
		}
		//class is mapped to the number of times it appears
		TreeMap<Integer, Integer> count = new TreeMap<>();
		int max = 0;
		int key = 0;
		for(float[] e: nn.values()){
			Integer c = count.get((int)e[0]);
			if(c==null)
				c = 0;
			c++;
			count.put((int)e[0], c);
			if(c>max){
				key = (int)e[0];
				max = c;
			}
		}
		
		return key;
	}

}

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
		TreeMap<Float, float[]> nn = new TreeMap<>();
		for(float[] trainS: trainingData){
			double d = getDistance(trainS, testS);
			nn.put((float)d, trainS);
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
		
//		// class is mapped to the number of times it appears
//		TreeMap<Integer, Float> c2D = new TreeMap<>();
//		TreeMap<Integer, Integer> c2c = new TreeMap<>();
//		float minD = 0;
//		int key = 0;
//		for (float d : nn.keySet()) {
//			Integer cl = (int) nn.get(d)[0];
//			Float c = c2D.get(cl);
//			Integer count = c2c.get(cl);
//			if (c == null)
//				c = 0f;
//			if (count == null)
//				count = 0;
//			c += d;
//			count++;
//			c2D.put(cl, c);
//			c2c.put(cl, count);
//		}
//		key = c2D.firstKey();
//		minD = c2D.get(key)/(float)c2c.get(key);
//		for (int c : c2D.keySet()) {
//			float d = c2D.get(c)/(float)c2c.get(c);
//			if (c < minD) {
//				key = c;
//				minD = d;
//			}
//		}
		
		return key;
	}

}

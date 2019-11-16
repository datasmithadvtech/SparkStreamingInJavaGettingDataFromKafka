package spark_s;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
public class r {
public static void main(String [] args)
{
	SparkConf s=new SparkConf();
	s.setMaster("local[*]");
	s.setAppName("PPPP");
	JavaSparkContext sc=new JavaSparkContext(s);
	
	JavaRDD<String> spam = sc.textFile("timeData.txt",1);
	JavaRDD<String> normal = sc.textFile("timeData.txt");
	
	// Create a HashingTF instance to map email text to vectors of 10,000 features.
	final HashingTF tf = new HashingTF(10000);
	// Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
	JavaRDD<LabeledPoint> posExamples = spam.map(new Function<String, LabeledPoint>() {
	public LabeledPoint call(String email) {
	return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
	}
	});
	JavaRDD<LabeledPoint> negExamples = normal.map(new Function<String, LabeledPoint>() {
	public LabeledPoint call(String email) {
	return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
	}
	});
	JavaRDD<LabeledPoint> trainData = negExamples.union(negExamples);
	trainData.cache(); // Cache since Logistic Regression is an iterative algorithm.
	// Run Logistic Regression using the SGD algorithm.
	LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());
	// Test on a positive example (spam) and a negative one (normal).
	Vector posTest = tf.transform(
	Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
	Vector negTest = tf.transform(
	Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));
	System.out.println("Prediction for positive example: " + model.predict(posTest));
	System.out.println("Prediction for negative example: " + model.predict(negTest));
}
}

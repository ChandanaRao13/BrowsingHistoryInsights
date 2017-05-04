package dataminingengine;

import java.net.UnknownHostException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class RecommendationTest {
	public static void main( String[] args ) throws UnknownHostException
    {
		SparkConf sparkConf = new SparkConf().setAppName("JavaRecommendationExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> data = sc.textFile("/PersonalFiles/MSSE/Semester3/CMPE239/Project/OtherDatasets/recommendation_test.txt");
        JavaRDD<Rating> ratings = data.map(s -> {
            String[] sarray = s.split(" ");
            return new Rating(Integer.parseInt(sarray[0]),
              Integer.parseInt(sarray[1]),
              Integer.parseInt(sarray[2]));
          });
        
        int rank = 10;
        int numIterations = 10;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);
        System.out.println("*******");
        //System.out.println("-----" + model.predict(2, 3) + "------" + model.recommendProductsForUsers(2));
        JavaRDD<Tuple2<Object, Rating[]>> products = model.recommendProductsForUsers(3).toJavaRDD();
       
        for (Tuple2<Object, Rating[]> rule
                : products.collect()) {
        		for(Rating rate : rule._2){
                System.out.println("User:::" + rule._1 + " ------  Chance of accessing::" + rate.rating() + " ----- URL:::" + rate.product() ) ;
        		}
                  
              }
        	    sc.stop();
        	    
   
        
        

    }
}

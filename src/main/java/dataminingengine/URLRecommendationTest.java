package dataminingengine;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class URLRecommendationTest {

	public List<String> getListOfRecommendedURLForUser(String user_id) {
		return null;
		
		// Get Data required data from mongo and from differnt collection.. Required format "user_id url num_of_clicks"
		// Maping url to url ID
		// Apply ALS
		// Return list of recommended urls and svae 

	}

	public void getMongoData() {
		SparkSession spark = SparkSession.builder()
				.master("local")
				.appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/browsinghistory.chromedata")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/browsinghistory.genericrules")
				.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaMongoRDD<Document> rdd = MongoSpark.load(sc);

		List<Document> pipeline =  Arrays.asList(Document.parse("{ $project: {user_id : 1 , _id:0, 'history.hostname':1}}"));

		JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(pipeline);
		System.out.println(aggregatedRdd.first().toJson());
		
		//Map Reduce to transfer into transactions
		
	}

}

package dataminingengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class URLRecommendation {
	public static void main( String[] args ) throws InterruptedException, ParseException
	{
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

		List<Document> list_doc = aggregatedRdd.collect();
		List<String> userId_urlList = new ArrayList<String>();

		for(Document doc : list_doc) {

			//To fetch single attribute in record
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(doc.toJson());			

			Object ob = json.get("history");
			List<Object> history = new ArrayList<Object>((List)ob);
			List<String> hostnameList = new ArrayList<String>();

			for (Object host : history) {
				JSONObject hostJson = (JSONObject) parser.parse(host.toString());
				String hostName = (String) hostJson.get("hostname");
				hostnameList.add(hostName);
				userId_urlList.add(json.get("user_id")+","+hostName);
			}

		}

		JavaRDD<String> lines = sc.parallelize(userId_urlList);
		JavaPairRDD<String,Integer> counts = lines
				.mapToPair(line -> new Tuple2<>(line,1))
				.reduceByKey((a,b)->a+b);
		counts.saveAsTextFile("output1.text");

		//aggregatedRdd.saveAsTextFile("/PersonalFiles/MSSE/Semester3/CMPE239/Project/OtherDatasets/sample.txt");




		//		 JavaMongoRDD<Document> aggRDD = rdd.withPipeline(Arrays.asList(Document.parse("{ _id: 5906e78096a09756988390d9}")));
		//		System.out.println(aggRDD.count());
		sc.close();

	}
}

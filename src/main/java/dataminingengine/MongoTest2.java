package dataminingengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mongodb.util.JSON;

public class MongoTest2 {
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
		 
//		System.out.println(rdd.count());
//		System.out.println(rdd.first().toJson());
//		String id = "100427714450058914900";
		
		List<Document> pipeline =  Arrays.asList(Document.parse("{ $project: {user_id : 1 , _id:0, 'history.hostname':1}}"));
		//List<Document> pipeline =  Arrays.asList(Document.parse("{ $project: {user_id : 1 , _id:0, history:1}},{$unwind:history},{ $project : {history : {$filter : {}}}}"));
		System.out.println("Pipeline:::" + pipeline.size());
	
		JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(pipeline);
		
		System.out.println("Count:::" + aggregatedRdd.count());
		System.out.println(aggregatedRdd.first().toJson());
		
		List<Document> list_doc = aggregatedRdd.collect();
		System.out.println("Display::::");
		for(Document doc : list_doc) {
			System.out.println("Entire doc**" + doc.toJson());
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(doc.toJson());
			System.out.println("userid::::" + json.get("user_id"));
			Object ob = json.get("history");
			List<Object> history = new ArrayList<Object>((List)ob);
			List<String> hostnameList = new ArrayList<String>();
			
			for (Object host : history) {
				JSONObject hostJson = (JSONObject) parser.parse(host.toString());
				String hostName = (String) hostJson.get("hostname");
				hostnameList.add(hostName);
				System.out.println("history:::::" + hostName);
			}
			
		}
		//aggregatedRdd.saveAsTextFile("/PersonalFiles/MSSE/Semester3/CMPE239/Project/OtherDatasets/sample.txt");
		

		 
		 
//		 JavaMongoRDD<Document> aggRDD = rdd.withPipeline(Arrays.asList(Document.parse("{ _id: 5906e78096a09756988390d9}")));
//		System.out.println(aggRDD.count());
		 sc.close();
		 
	    }
}

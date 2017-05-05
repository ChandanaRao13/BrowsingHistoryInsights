package dataminingengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class URLRecommendation {
	public static void main( String[] args ) throws InterruptedException, ParseException
	{
		SparkSession sparkSessionObject = SparkSession.builder()
				.master("local")
				.appName("URLRecommendationComponent")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/browsinghistory.chromedata")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/browsinghistory.genericrules")
				.getOrCreate();
		
		JavaSparkContext sparkContextObject = new JavaSparkContext(sparkSessionObject.sparkContext());

		JavaMongoRDD<Document> browsingHistoryRDD = MongoSpark.load(sparkContextObject);

		List<Document> filteredRecords =  Arrays.asList(Document.parse("{ $project: {user_id : 1 , _id:0, 'history.hostname':1}}"));
		JavaMongoRDD<Document> aggregatedBrowsingHistoryRDD = browsingHistoryRDD.withPipeline(filteredRecords);
		List<Document> recordsList = aggregatedBrowsingHistoryRDD.collect();
		
		List<String> userId_URL_List = new ArrayList<String>();
		
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "URLRepository");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(sparkContextObject).withOptions(readOverrides);
		JavaMongoRDD<Document> URLMappingRDD = MongoSpark.load(sparkContextObject,readConfig);
		
		JSONParser parser = new JSONParser();
		JSONObject json;
		Object ob;
		
		for(Document doc : recordsList) {	
			//To fetch single attribute in record
			json = (JSONObject) parser.parse(doc.toJson());			

			ob = json.get("history");
			List<Object> history = new ArrayList<Object>((List)ob);
			
			List<String> hostnameList = new ArrayList<String>();

			for (Object host : history) {
				JSONObject chromeDataJSON = (JSONObject) parser.parse(host.toString());
				String hostName = (String) chromeDataJSON.get("hostname");
				hostnameList.add(hostName);
				
				String query = "{$match : {URLDomainName : \""+hostName+"\"}}";
				List<Document> filteredURLRecords = Arrays.asList(Document.parse(query));
				JavaMongoRDD<Document> aggregatedURLRDD = URLMappingRDD.withPipeline(filteredURLRecords);
				List<Document> urlRecordsList = aggregatedURLRDD.collect();
				JSONObject json1 = (JSONObject) parser.parse(urlRecordsList.get(0).toJson());	
				Object ob1 = json1.get("URLId");
					
				userId_URL_List.add(json.get("user_id")+" "+ob1);
			}

		}

		JavaRDD<String> lines = sparkContextObject.parallelize(userId_URL_List);
		JavaPairRDD<String,Integer> counts = lines
				.mapToPair(line -> new Tuple2<>(line,1))
				.reduceByKey((a,b)->a+b);
		JavaRDD<String> output = counts.map(a -> a._1 + " " + a._2);
		output.saveAsTextFile("output.txt");
		
		JavaRDD<String> data = sparkContextObject.textFile("output.txt");
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
        JavaRDD<Tuple2<Object, Rating[]>> products = model.recommendProductsForUsers(7).toJavaRDD();
       
        for (Tuple2<Object, Rating[]> rule
                : products.collect()) {
        		for(Rating rate : rule._2){
                System.out.println("User:::" + rule._1 + " ------  Chance of accessing::" + Math.round(rate.rating()) + " ----- URL:::" + rate.product() ) ;
        		}
                  
              }
		
		
		sparkContextObject.close();

	}
}

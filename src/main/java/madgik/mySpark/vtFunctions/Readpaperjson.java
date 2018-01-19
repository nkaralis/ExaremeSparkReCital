package madgik.mySpark.vtFunctions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Readpaperjson implements ExaremeVtFunction{

	private String filePath;
	
	public Readpaperjson(String path) {
		this.filePath = path;
	}
	
	@Override
	public String mapReduce(SparkSession spark) {
		
		Dataset<Row> articles = spark.read().json(filePath);
		
		articles.limit(1).createOrReplaceTempView("articles");
		return "articles";
	}

}

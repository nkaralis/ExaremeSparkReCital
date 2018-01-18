package madgik.mySpark.vtFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import madgik.mySpark.parser.exception.VtExtensionParserCancelationException;

public class Metadata {

	private String filePath;
	
	public Metadata(String filePath) {
		super();
		this.filePath = filePath;
	}
	
	public String mapReduce(SparkSession spark) throws VtExtensionParserCancelationException{
		try{
			Dataset<Row> metadata = spark.read().json(filePath);
	
			// Creates a temporary view using the DataFrame
			metadata.select("id", "title").limit(500).createOrReplaceTempView("metadata");
			
			return "metadata";
		}catch(Exception e){
			throw new VtExtensionParserCancelationException(e.getMessage());
		}
		
	}
	
	
	
	
}

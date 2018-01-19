package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import madgik.mySpark.parser.exception.VtExtensionParserCancelationException;

//it simply reads a txt with 3 fields(window,trigram,match) as strings separated by comma
//select * from rdemomatch('../demovalidation/demomatch') 
public class Rdemomatch implements ExaremeVtFunction {

	private String filepath;
	
	public Rdemomatch(String filePath) {
		super();
		this.filepath = filePath;
	}

	public String getFilePath() {
		return filepath;
	}

	public void setFilePath(String filePath) {
		this.filepath = filePath;
	}
	@Override 
	public String mapReduce(SparkSession spark) throws VtExtensionParserCancelationException{
		try{
			// Create an RDD
			JavaRDD<String> peopleRDD = spark.sparkContext()
			  .textFile(filepath, 1)
			  .toJavaRDD();
			// The schema is encoded in a string
			String schemaString = "window metadata_trigram metadata_ids";
	
			// Generate the schema based on the string of schema
			List<StructField> fields = new ArrayList<StructField>();
			for (String fieldName : schemaString.split(" ")) {
			  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			  fields.add(field);
			}
			StructType schema = DataTypes.createStructType(fields);
	
			// Convert records of the RDD (people) to Rows
			JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
				  String[] attributes = record.split("--");
				
				  return RowFactory.create(attributes[0], attributes[1],attributes[2]);
			});
	
			
			// Apply the schema to the RDD
			Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
	
			// Creates a temporary view using the DataFrame
			peopleDataFrame.limit(50000).createOrReplaceTempView("match");
			
			return "match";
		}catch(Exception e){
			throw new VtExtensionParserCancelationException(e.getMessage());
		}
		
	}
}
	
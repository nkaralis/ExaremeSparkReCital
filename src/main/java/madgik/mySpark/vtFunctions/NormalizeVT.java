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
//exaremeSQL> select * from normalizeudf(',','../demo')

import madgik.mySpark.parser.exception.VtExtensionParserCancelationException;


public class NormalizeVT implements ExaremeVtFunction{
	
	private String delimeter;
	private String filePath;
	
	public NormalizeVT(String delimeter, String filePath) {
		super();
		this.delimeter = delimeter;
		this.filePath = filePath;
	}

	public String getDelimeter() {
		return delimeter;
	}

	public void setDelimeter(String delimeter) {
		this.delimeter = delimeter;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public String mapReduce(SparkSession spark) throws VtExtensionParserCancelationException{
			
			// Create an RDD
			JavaRDD<String> peopleRDD = spark.sparkContext()
			  .textFile(filePath, 1)
			  .toJavaRDD();
			// The schema is encoded in a string
			String schemaString = "id title";
	
			// Generate the schema based on the string of schema
			List<StructField> fields = new ArrayList<StructField>();
			for (String fieldName : schemaString.split(" ")) {
			  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			  fields.add(field);
			}
			StructType schema = DataTypes.createStructType(fields);
	
			// Convert records of the RDD (people) to Rows
			JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
				  String[] attributes = record.split(",");
				  return RowFactory.create(attributes[0],attributes[1].toLowerCase().replaceAll("[^a-zA-z0-9 ]","_").replaceAll(" +"," "));
			});
	
			
			// Apply the schema to the RDD
			Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
			
			// Creates a temporary view using the DataFrame
			peopleDataFrame.limit(2).createOrReplaceTempView("titles");
			
			return "titles";
		
		
	}
	

}

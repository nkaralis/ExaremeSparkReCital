package madgik.mySpark.vtFunctions;

import java.io.FileNotFoundException;
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
import org.apache.spark.sql.SQLContext;

//select * from bagofwords('../jsonfolder/PMC.23.json')
public class Bagofwords {

	private String filePath;
	
	public Bagofwords(String filePath) {
		super();
		this.filePath = filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public String mapReduce(SparkSession spark) throws VtExtensionParserCancelationException{
		try{
			// Create an RDD
			JavaRDD<String> peopleRDD = spark.sparkContext()
			  .textFile(filePath, 1)
			  .toJavaRDD();
			// The schema is encoded in a string
			String schemaString = "id bagofwords";
	
			// Generate the schema based on the string of schema
			List<StructField> fields = new ArrayList<StructField>();
			for (String fieldName : schemaString.split(" ")) {
			  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			  fields.add(field);
			}
			StructType schema = DataTypes.createStructType(fields);
			
			Dataset<Row> articlesDataFrame = spark.read().json(this.filePath);
			ArrayList<Row> output_rows = new ArrayList<Row>();
			//2 -> publication id
			//3 -> journalTitle
			//6 -> pubYear
			//8 -> title of article
			for(Row r : articlesDataFrame.collectAsList()) {
				output_rows.add(RowFactory.create(r.getString(2), r.getString(3)+ " %% "+ r.getString(6)+" %% " +r.getString(8)));
			}
			
			
			// Apply the schema to the RDD
			Dataset<Row> bagofwordsDataFrame = spark.createDataFrame(output_rows, schema);
	
			// Creates a temporary view using the DataFrame
			bagofwordsDataFrame.limit(200).createOrReplaceTempView("people");
			
			return "people";
		}catch(Exception e){
			throw new VtExtensionParserCancelationException(e.getMessage());
		}
		
	}
	
	
	
	
}

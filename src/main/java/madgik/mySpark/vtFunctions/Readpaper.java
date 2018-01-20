package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.List;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Readpaper  implements ExaremeVtFunction {
	
	private String filepath;
	
	public Readpaper(String filePath) {
		super();
		this.filepath = filePath;
		
	}
//	select line,findsignal(line) as signal from readpaper('../demopapers/demopaper1.txt')
	@Override
	public String mapReduce(SparkSession spark){
		// Create an RDD
		JavaRDD<String> readpaperRDD = spark.sparkContext()
		  .textFile(filepath,1)
		  .toJavaRDD();
		
		// The schema is encoded in a string
		String schemaString = "line";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		ArrayList<Row> readpaper = new ArrayList<Row>();
		for(String line:readpaperRDD.collect()) {
			if (line.length() >=10 && !(line.trim().isEmpty()))
				readpaper.add(RowFactory.create(line));
		}
		
		

		// finally we create dataset and view
		Dataset<Row> output_dataset = spark.createDataFrame(readpaper, schema);
		output_dataset.createOrReplaceTempView("readpaper");
		
		return "readpaper";

	}
}

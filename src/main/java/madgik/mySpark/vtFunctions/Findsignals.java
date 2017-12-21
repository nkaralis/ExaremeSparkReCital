package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
//import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//updated findsignals now cleans empty lines or lines with less than 10 characters

public class Findsignals implements ExaremeVtFunction{
	
	private String filepath;
	private String pattern;
	
	public Findsignals(String filePath) {
		super();
		this.filepath = filePath;
		//this.pattern = "((.*)1[5-9]\\d{2,2}[.| *](.*))|((.*)20\\d{2,2}[.| *](.*))|((.*)et al(.*))|((.*)http(.*))";
		this.pattern = "19\\d{2}|20\\d{2}|((.*)et al(.*))|((.*)http(.*))";
	}
	
	@Override
	public String mapReduce(SparkSession spark){
		// Create an RDD
		JavaRDD<String> signalRDD = spark.sparkContext()
		  .textFile(filepath,1)
		  .toJavaRDD();
		
		// The schema is encoded in a string
		String schemaString = "line signal";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		//for regular expressions
		
		ArrayList<Row> densearray = new ArrayList<Row>();
		Pattern r = Pattern.compile(this.pattern);
		//now let's convert lines from input file to rows
		for(String line:signalRDD.collect()) {
			if (line.length() >=10 && !(line.trim().isEmpty())) { //for the case that line has only spaces
				Matcher matcher = r.matcher(line);
				int result = -1;
				if (matcher.find())
						result = 1;
				else
						result = 0;
				//now add a new row to result containing the initial row of the file and the result of regex matching
				densearray.add(RowFactory.create(line,Integer.toString(result)));
			}
		}
		
		

		// finally we create dataset and view
		Dataset<Row> output_dataset = spark.createDataFrame(densearray, schema);
		output_dataset.limit(10).createOrReplaceTempView("finddensities");
		return "finddensities";
	}
}

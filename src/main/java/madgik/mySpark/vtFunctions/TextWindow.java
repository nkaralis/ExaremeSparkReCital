package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import madgik.mySpark.parser.exception.VtExtensionParserCancelationException;

public class TextWindow implements ExaremeVtFunction {
	
	private int prev;
	private int mid;
	private int next;
	private Dataset<Row> dataset;
	
	public TextWindow(Dataset<Row> d, int p, int m, int n) {
		dataset = d;
		prev = p;
		mid = m;
		next = n;
	}

	@Override
	public String mapReduce(SparkSession spark) {
		
		// The schema is encoded in a string
		String schemaString = "p_id title_id previous middle next";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		
		
		return null;
	}

}

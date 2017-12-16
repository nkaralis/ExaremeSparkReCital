package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Charinvindex implements ExaremeVtFunction {
	
	private Dataset<Row> input_dataset;
	
	public Charinvindex(Dataset<Row> ds) {
		input_dataset = ds;		
	}
	
	@Override
	public String mapReduce(SparkSession spark) {
		// The schema is encoded in a string
		String schemaString = "trigram title_id";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		// Iterate rows of input dataset
		HashMap<String, ArrayList<String>> inv_index = new HashMap<String, ArrayList<String>>();
		// Create inverted index
		for (Row r : input_dataset.select("title_id", "middle").collectAsList()) {
			String trigram = r.getString(1);
			String id = r.getString(0);
			if (!inv_index.containsKey(trigram)) {
				inv_index.put(trigram, new ArrayList<String>());
				inv_index.get(trigram).add(id);
			}
			else {
				if (!inv_index.get(trigram).contains(id))
					inv_index.get(trigram).add(id);
			}
		}
		
		// Create list of rows from the inverted_index
		ArrayList<Row> rows = new ArrayList<Row>();
		for(String key : inv_index.keySet()) {
			rows.add(RowFactory.create(key, String.join(",", inv_index.get(key))));
		}
		// create dataset and view
		Dataset<Row> inv_index_dataset = spark.createDataFrame(rows, schema);
		
		// start building characteristic inverted index
		// Dataset<Row> char_inv_index_dataset = inv_index_dataset.filter(functions.length(inv_index_dataset.col("title_id").equalTo(1)));
		//inv_index_dataset.filter(functions.length(inv_index_dataset.col("title_id").equalTo('1'))).limit(100).createOrReplaceTempView("charinvindex");
		inv_index_dataset.limit(100).createOrReplaceTempView("charinvindex");
		return "charinvindex";
	}

}

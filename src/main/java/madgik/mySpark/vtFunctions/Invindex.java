package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Invindex implements ExaremeVtFunction {
	
private Dataset<Row> input_dataset;
	
	public Invindex(Dataset<Row> ds) {
		input_dataset = ds;		
	}

	@Override
	public String mapReduce(SparkSession spark) {
		
		// The schema is encoded in a string
		String schemaString = "trigram title_id";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("trigram", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("title_id", DataTypes.createArrayType(DataTypes.StringType), true));
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
			rows.add(RowFactory.create(key, inv_index.get(key).toArray()));
		}
		// Create dataset for the inverted_index
		Dataset<Row> inv_index_dataset = spark.createDataFrame(rows, schema);
		inv_index_dataset.sort("trigram").createOrReplaceTempView("invindex");
		
		return "invindex";
	}

}

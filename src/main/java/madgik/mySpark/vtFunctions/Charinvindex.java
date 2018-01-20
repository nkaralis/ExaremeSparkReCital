package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.reflect.internal.util.Set;

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
		fields.add(DataTypes.createStructField("trigram", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("title_id", DataTypes.createArrayType(DataTypes.StringType), true));
		StructType schema = DataTypes.createStructType(fields);
				
		// Start building characteristic inverted index
		Dataset<Row> char_inv_index_dataset = spark.createDataFrame(new ArrayList<Row>(), schema);
		TreeSet<String> titles = new TreeSet<String>();
		TreeSet<String> titles_to_be_removed = new TreeSet<String>();
		for(Row r : input_dataset.select("title_id").collectAsList()) {
			for(Object s : r.getList(0)) {
				titles.add((String)s);
			}
		}
		int threshold = 1;
		while(!titles.isEmpty()) {
			ArrayList<Row> new_rows = new ArrayList<Row>();
			Dataset<Row> temp = input_dataset.filter(functions.size(input_dataset.col("title_id")).equalTo(threshold));
			if(temp.count() > 0) {
				threshold = 1;
				char_inv_index_dataset = char_inv_index_dataset.union(temp).dropDuplicates("title_id");
				for(Row r : temp.select("title_id").collectAsList()) {
					titles_to_be_removed.addAll(r.getList(0));
				}
				titles.removeAll(titles_to_be_removed);
				for(Row r : input_dataset.collectAsList()) {
					List<String> new_list = r.getList(1);
					ArrayList<String> temp_arraylist = new ArrayList<String>(new_list);
					temp_arraylist.removeAll(titles_to_be_removed);
					if(new_list.size() > 0) new_rows.add(RowFactory.create(r.getString(0), temp_arraylist));
				}
				input_dataset = spark.createDataFrame(new_rows, schema);
			}
			else {
				threshold ++;
			}
		}
		
		char_inv_index_dataset.createOrReplaceTempView("charinvindex");
		return "charinvindex";
	}

}

package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Textwindow implements ExaremeVtFunction {
	
	private int prev;
	private int mid;
	private int next;
	private String text_col;
	private String id_col;
	private String delim;
	private Dataset<Row> input_dataset;
	
	public Textwindow(Dataset<Row> ds, String p, String m, String n, String d, String col1, String col2) {
		input_dataset = ds;
		delim = d;
		id_col = col1;
		text_col = col2;
		prev = Integer.parseInt(p);
		mid = Integer.parseInt(m);
		next = Integer.parseInt(n);
	}
	
	public Textwindow(Dataset<Row> ds, String p, String m, String n, String d, String col) {
		input_dataset = ds;
		delim = d;
		id_col = null;
		text_col = col;
		prev = Integer.parseInt(p);
		mid = Integer.parseInt(m);
		next = Integer.parseInt(n);
	}

	@Override
	public String mapReduce(SparkSession spark) {
		
		// The schema is encoded in a string
		String schemaString = "title_id previous middle next";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
			
		// Iterate rows of input dataset
		ArrayList<Row> textwindow = new ArrayList<Row>();
		if(id_col != null) {
			for (Row r : input_dataset.select(text_col, id_col).collectAsList()){
				String t = r.getString(0); // get text / title
				String t_id = r.getString(1); // get article id
				String[] tokens = t.split(delim); // split title into tokens using the specified delimeter
				// textwindow2s
				for(int i = 0; i < tokens.length-mid+1; i++) {
					int im = i+mid;
					String previous;
					if(i-prev < 0)
						previous = String.join(" ", Arrays.copyOfRange(tokens, 0, i));
					else
						previous = String.join(" ", Arrays.copyOfRange(tokens, i-prev, i));
					String middle = String.join(" ", Arrays.copyOfRange(tokens, i, im));
					String next = String.join(" ", Arrays.copyOfRange(tokens, im, im+this.next));
					textwindow.add(RowFactory.create(t_id, previous, middle, next));
				}	
			}
		}
		else {
			for (Row r : input_dataset.select(text_col).collectAsList()){
				String t = r.getString(0); // get text / title
				String[] tokens = t.split(delim); // split title into tokens using the specified delimeter
				// textwindow2s
				for(int i = 0; i < tokens.length-mid+1; i++) {
					int im = i+mid;
					String previous;
					if(i-prev < 0)
						previous = String.join(" ", Arrays.copyOfRange(tokens, 0, i));
					else
						previous = String.join(" ", Arrays.copyOfRange(tokens, i-prev, i));
					String middle = String.join(" ", Arrays.copyOfRange(tokens, i, im));
					String next = String.join(" ", Arrays.copyOfRange(tokens, im, im+this.next));
					textwindow.add(RowFactory.create(null, previous, middle, next));
				}	
			}
		}
		// create dataset and view
		Dataset<Row> output_dataset = spark.createDataFrame(textwindow, schema);
		output_dataset.createOrReplaceTempView("textwindow2s");
		return "textwindow2s";
	}

}

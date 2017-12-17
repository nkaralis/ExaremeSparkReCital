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

public class Density implements ExaremeVtFunction{

	private Dataset<Row> input_dataset;
	private int window;
	
	public Density(Dataset<Row> ds, String w) {
		input_dataset = ds;
		window = Integer.parseInt(w);
	}
	
	@Override
	public String mapReduce(SparkSession spark) {
		
		// The schema is encoded in a string
		String schemaString = "line signal density";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		// Preparation for window
		int[] signals = new int[input_dataset.collectAsList().size()];
		String[] lines = new String[signals.length];
		double[] densities = new double[signals.length];
		int counter = 0;
		for(Row r : input_dataset.collectAsList()) {
			lines[counter] = r.getString(0);
			signals[counter] = Integer.parseInt(r.getString(1));
			counter++;
		}
		// Window over signals column
		for(int i = 0; i < signals.length-window+1; i++) {
			int iw = i + window;
			int sum = 0;
			// Calculate the sum of the signals in the window
			for(int j = i; j < iw; j++) {
				if(signals[j] == 1) sum ++;
			}
			// Set density
			double cur_density = (double) sum/window;
			for(int j = i; j < iw; j++) {
				if(densities[j] < cur_density) densities[j] = cur_density;
			}			
		}
		// Return line with density gt avg_density
		double sum_densities = 0.0;
		for(int i = 0; i < densities.length; i++) {
			sum_densities += densities[i];
		}
		double avg_density= sum_densities/densities.length;
		ArrayList<Row> output_rows = new ArrayList<Row>();
		for(int i = 0; i < signals.length; i++) {
			if(densities[i] >= avg_density) {
				output_rows.add(RowFactory.create(lines[i], String.valueOf(signals[i]), String.valueOf(densities[i])));
			}
		}
		Dataset<Row> output_dataset = spark.createDataFrame(output_rows, schema);
		output_dataset.limit(50).createOrReplaceTempView("densities");
		return "densities";
	}

}

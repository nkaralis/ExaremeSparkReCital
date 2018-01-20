package madgik.mySpark.vtFunctions;

import java.util.ArrayList;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//execution example
//select * from density((select * from findsignals('../demo.txt')),2,1,2)
// select * from density((select line,findsignal(line) as signal from readpaper('../demopapers/demopaper1.txt')),2,1,2)
public class Density implements ExaremeVtFunction{

	private Dataset<Row> input_dataset;
	private int prev;
	private int mid;
	private int next;
	private int w_size;
	
	public Density(Dataset<Row> ds, String p, String m, String n) {
		input_dataset = ds;
		prev = Integer.parseInt(p);
		mid = Integer.parseInt(m);
		next = Integer.parseInt(n);
		w_size = prev+mid+next;
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
		//input_dataset.show();
		for(Row r : input_dataset.collectAsList()) {
			
			lines[counter] = r.getString(0);
			//signals[counter] = Integer.parseInt(r.getString(1)); //if we read from a vtable
			signals[counter] = r.getInt(1); //if we read from udf findsignal
			counter++;
		}
		// Window over signals column
		// Middle part of the window
		for(int i = 0; i < signals.length-mid+1; i++) {
			int iw = i + mid;
			int sum = 0;
			// Previous part of the window
			if(i-prev >= 0) {
				for(int x = i-prev; x < i; x++) {
					if(signals[x] == 1) sum++;
				}
			}
			else {
				//initially go from start to mid(here it is 1)
				int count = 0;
				for (int x=0;x<mid;x++) {
					if(signals[x] == 1) 
						sum++;
					count++;
				}
				//take leftovers after the next
				for (int x=i+next; x<=i+next+count;x++){		
					if(signals[x] == 1) sum++;
				}
			
			}
			// Middle part of the window
			for(int x = i; x < i+mid; x++) {
				if(signals[x] == 1) sum++;
			}
			// Next part of the window
			if(i+mid+next <= signals.length) {
				for(int x = i+mid; x < i+mid+next; x++) {
					if(signals[x] == 1) sum++;
				}
			}
			else {
				//initially go from mid to end of file
				int count2 = 0;
				for (int x= i+mid;x<signals.length;x++) {
					count2++;
					if(signals[x] == 1) sum++;
				}
				//take leftovers from previous to previous elements
				for (int x=i-prev-next+count2;x<i-prev;x++) {
					if(signals[x] == 1) sum++;
				}
				
			}
			// Set density
			double cur_density = (double) sum/w_size;
			for(int x = i; x < iw; x++) {
				if(densities[x] < cur_density) densities[x] = cur_density;
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
		
		output_dataset.createOrReplaceTempView("densities");
		return "densities";
	}

}
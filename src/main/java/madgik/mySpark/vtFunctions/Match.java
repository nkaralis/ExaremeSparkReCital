package madgik.mySpark.vtFunctions;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Match implements ExaremeVtFunction {
	
	private Dataset<Row> references;
	private Dataset<Row> char_inv_index;
	
	public Match(Dataset<Row> ids1, Dataset<Row> ids2) {
		this.references = ids2;
		this.char_inv_index = ids1;
	}
	
	@Override
	public String mapReduce(SparkSession spark) {
		
		// Generate the schema
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("window", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("metadata_trigram", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("metadata_id", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		
		// find matches
		Dataset<Row> matches = references.join(char_inv_index, references.col("middle").equalTo(char_inv_index.col("trigram")));
		
		// return proper schema
		ArrayList<Row> matchTemp = new ArrayList<Row>();
		for(Row r: matches.collectAsList()) {
			for (Object id : r.getList(5)) {
				matchTemp.add(RowFactory.create((r.getString(1)+" "+r.getString(2)+" "+r.getString(3)), r.getString(4), (String) id));
			}
		}
		
		Dataset<Row> matches_final = spark.createDataFrame(matchTemp, schema);
		matches_final.createOrReplaceTempView("matches");
		return "matches";
	}

}

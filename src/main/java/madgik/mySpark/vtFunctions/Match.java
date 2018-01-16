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
		
		Dataset<Row> matches = references.join(char_inv_index, references.col("middle").equalTo(char_inv_index.col("trigram")));
		
		matches.createOrReplaceTempView("matches");
		return "matches";
	}

}

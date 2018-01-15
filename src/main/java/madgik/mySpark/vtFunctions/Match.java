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
		this.references = ids1;
		this.char_inv_index = ids2;
	}
	
	@Override
	public String mapReduce(SparkSession spark) {
		// The schema is encoded in a string
		String schemaString = "reference md_article_id";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("reference", DataTypes.StringType, true));
		//fields.add(DataTypes.createStructField("title", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("md_article_id", DataTypes.createArrayType(DataTypes.StringType), true));
		StructType schema = DataTypes.createStructType(fields);
		
		// Matching phase
		ArrayList<Row> matches = new ArrayList<Row>();
		for(Row ref : references.collectAsList()) { // iterate over the trigrams of the reference section
			
			for(Row metadata : char_inv_index.collectAsList()) { // iterate over the trigrams of the metadata
				//System.out.println(ref.getString(2)+" || "+metadata.getString(0));
				if(ref.getString(2).compareTo(metadata.getString(0)) == 0) {
					String temp = ref.getString(3);
					matches.add(RowFactory.create(temp, metadata.getList(1)));
				}
			}
			
		}
		Dataset<Row> matchesDataset = spark.createDataFrame(matches, schema);
		matchesDataset.createOrReplaceTempView("matches");
		return "matches";
	}

}

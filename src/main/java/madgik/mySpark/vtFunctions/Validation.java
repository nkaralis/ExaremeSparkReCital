package madgik.mySpark.vtFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//select * from validation( (select * from rdemomatch('../demovalidation/demomatch')) ,(select * from bagofwords('../demovalidation/PMC.23.json' )))
public class Validation implements ExaremeVtFunction {
	
	private Dataset<Row> match_fields;
	private Dataset<Row> metadata_bow;
	
	public Validation(Dataset<Row> ids1, Dataset<Row> ids2) {
		this.match_fields = ids1;
		this.metadata_bow = ids2;
	}
	//initially i want to perform the join
	@Override
	public String mapReduce(SparkSession spark) {
		
		Dataset<Row> validityDataFrame = match_fields.join(metadata_bow, match_fields.col("metadata_ids").equalTo(metadata_bow.col("id")));
		
		validityDataFrame.createOrReplaceTempView("validity");
		validityDataFrame.show();
		return "validity";
	}

}

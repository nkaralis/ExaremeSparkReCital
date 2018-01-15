package madgik.mySpark.vtFunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Articleid implements ExaremeVtFunction {
	
	private String article;
	
	public Articleid(String path) {
		this.article = path;
	}

	@Override
	public String mapReduce(SparkSession spark) {
		// The schema is encoded in a string
		String schemaString = "article_id";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("article_id", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		
		// Create an RDD
		JavaRDD<String> articleRDD = spark.sparkContext()
		  .textFile(article, 1)
		  .toJavaRDD();
		
		// Get the id of the article
		String id = articleRDD.collect().get(0).split("\t")[0];
		ArrayList<Row> idList = new ArrayList<Row>();
		idList.add(RowFactory.create(id));
		Dataset<Row> output_dataset = spark.createDataFrame(idList, schema);
		output_dataset.createOrReplaceTempView("id");
		return "id";
	}

}

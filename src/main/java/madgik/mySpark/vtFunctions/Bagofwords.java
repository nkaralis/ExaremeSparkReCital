package madgik.mySpark.vtFunctions;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import madgik.mySpark.parser.exception.VtExtensionParserCancelationException;
import org.apache.spark.sql.SQLContext;

//select * from bagofwords('../jsonfolder/PMC.23.json')
public class Bagofwords {

	private String filePath;
	
	public Bagofwords(String filePath) {
		super();
		this.filePath = filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public String GetAuthors(String line) {
		String final1="";
        String[] list ;
		if (line!=null){
            list = line.split(","); // list[0] = "LastName":"Planansky"
            for (int index = 1 ; index < list.length; index+=3){
               String[] list1;
               list1 = list[index].split(":");
               final1 = final1 + list1[1].replaceAll("\"","")+ " ";
            }
            return final1;
        }else
            return "null";
	}
	public String mapReduce(SparkSession spark) throws VtExtensionParserCancelationException{
		try{
		
			// The schema is encoded in a string
			String schemaString = "id bagofwords";
			List<StructField> fields = new ArrayList<StructField>();
			fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("bagofwords", DataTypes.createArrayType(DataTypes.StringType), true));
			StructType schema = DataTypes.createStructType(fields);
			
			Dataset<Row> articlesDataFrame = spark.read().json(this.filePath);
			
			Dataset<Row> authors = articlesDataFrame.select("id","AuthorList.Author","journalTitle","pubYear");//.createOrReplaceTempView("people");
			ArrayList<Row> output_rows = new ArrayList<Row>();
			for(Row r : authors.collectAsList()) {
				List<String> new_list = new ArrayList<String>() ;
				new_list.add(r.getString(2));  //journal title
				new_list.add(r.getString(3)); //pubyear
				new_list.add(GetAuthors(r.getString(1))); //author surnames
				ArrayList<String> temp_arraylist = new ArrayList<String>(new_list);
				output_rows.add(RowFactory.create(r.getString(0),temp_arraylist));
			}
		
			// Apply the schema to the RDD
			Dataset<Row> bagofwordsDataFrame = spark.createDataFrame(output_rows, schema);
	
			// Creates a temporary view using the DataFrame
			bagofwordsDataFrame.createOrReplaceTempView("people");
			
			return "people";
		}catch(Exception e){
			throw new VtExtensionParserCancelationException(e.getMessage());
		}
		
	}
	
	
	
	
}

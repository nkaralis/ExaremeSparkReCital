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

import madgik.mySpark.parser.exception.VtExtensionParserCancelationException;


//select * from bagofwords('../jsonfolder/PMC.23.json')
//spark's json reader reads only first 18721 lines of PMC.23.json
public class Bagofwords implements ExaremeVtFunction{

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
            //System.out.println(final1);
            return final1;
        }else
            return "null";
	}
	public String mapReduce(SparkSession spark) throws VtExtensionParserCancelationException{
		try{
		
			// The schema is encoded in a string
			
			List<StructField> fields = new ArrayList<StructField>();
			fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("title", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("journalTitle", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("pubYear", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("Authors", DataTypes.StringType, true));
			//fields.add(DataTypes.createStructField("bagofwords", DataTypes.createArrayType(DataTypes.StringType), true));
			StructType schema = DataTypes.createStructType(fields);
			
			Dataset<Row> articlesDataFrame = spark.read().json(this.filePath);
			
			Dataset<Row> authors = articlesDataFrame.select("id","title","AuthorList.Author","journalTitle","pubYear");//.createOrReplaceTempView("people");
			
			//authors.show();
			ArrayList<Row> output_rows = new ArrayList<Row>();
			
			for(Row r : authors.collectAsList()) {
			
			/*List<String> new_list = new ArrayList<String>() ;
				new_list.add(r.getString(2));  //journal title
				new_list.add(r.getString(3)); //pubyear
				System.out.println(r.getString(1));
				//GetAuthors(r.getString(1));
				//new_list.add(GetAuthors(r.getString(1))); //author surnames
				ArrayList<String> temp_arraylist = new ArrayList<String>(new_list);
			*/ 
				
				output_rows.add(RowFactory.create(r.getString(0),r.getString(1),r.getString(3),r.getString(4),GetAuthors(r.getString(2))));
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

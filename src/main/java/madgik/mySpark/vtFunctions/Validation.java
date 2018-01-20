package madgik.mySpark.vtFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/*
select * from validation((select * from rdemomatch('../demovalidation/demomatch')),(select * from bagofwords('../demovalidation/PMC.23.json')))
*/
public class Validation implements ExaremeVtFunction {
	
	private Dataset<Row> match_fields;
	private Dataset<Row> metadata_bow;
	
	public Validation(Dataset<Row> ids1, Dataset<Row> ids2) {
		this.match_fields = ids1;
		this.metadata_bow = ids2;
	}
	//initially i want to perform the join - check
	//now i want to get elements for regex functions
	//works for one match
	
	public  String comprspaces(String str1,String str2){
   		
   		if (str2 =="Null")
            return str1.trim().replaceAll(" +"," ");
         
         else
            return str1.trim().replaceAll(" +"," ") +" "+ str2.trim().replaceAll(" +"," ");
         
    }
	
	public int regexpcountwords(String pattern,String expression){
        Pattern myPattern = Pattern.compile(pattern, Pattern.UNICODE_CHARACTER_CLASS);
        Matcher myMatcher = myPattern.matcher(expression);
        int sum = 0;
        int occurences ;
        while (myMatcher.find()) {
              // Do your stuff here
              occurences = 0; 
              System.out.println(myMatcher.group());
             // occurences = StringUtils.countMatches(myMatcher.group().trim()," ") + 1;
              sum = sum + occurences;
              //sum = sum + myMatcher.group().trim().count(' ')+1;
        }
        return sum;

	}
	
	public  String regexpr(String pattern,String expression,String replace_expression){
        int count = 0,i;
        String ret_str = "";
        if (!pattern.equals("NULL"))
           count = count + 1;
        if (!expression.equals("NULL"))
           count = count + 1;
        if (!replace_expression.equals("NULL"))
           count = count + 1;

        //case we have less than two arguments
        if (count<2)
           return "Null";
        else if (count==2){        //case we have exactly two arguments
           Pattern myPattern = Pattern.compile(pattern, Pattern.UNICODE_CHARACTER_CLASS);
           Matcher myMatcher = myPattern.matcher(expression);
           if (myMatcher.find()){
              if (myMatcher.groupCount()>0){  //While matcher finds something append concatenate it with return string
                 int groupCount = myMatcher.groupCount();
                  for (i = 1; i <= groupCount; i++) 
                       ret_str = ret_str + myMatcher.group(i) +" ";
                  return ret_str;
              }
              else
                 return "True";
           }
           else
              return "Null";
        }
        else                     //case we have three arguments
           return expression.replaceAll(pattern,replace_expression);
	}
	
	public static double regexpcountdistance(String pattern,String expression){
	      double count = 0.0;
	     
	      Pattern myPattern = Pattern.compile(pattern, Pattern.UNICODE_CHARACTER_CLASS);
	      Matcher myMatcher = myPattern.matcher(expression);
	      int exlength = expression.split(" ").length;
	      while (myMatcher.find()){
	          count = count + 1.0/exlength;
	      }
	
	      return count;
	 }
	
	public static double calc(int n) {
		double sum = 0.0;
		for (int i=1;i<=(int)(n/2)+1;i++)
			sum = sum + 1.0/i;
		sum = 2*sum;
		return sum;
	}
	
	@Override
	public String mapReduce(SparkSession spark) {
		
		//return schema table
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("window", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("trigram", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("metadata_id", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("confidence", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> validityDataFrame = match_fields.join(metadata_bow, match_fields.col("metadata_ids").equalTo(metadata_bow.col("id")));
		//validityDataFrame.show();
		//validityDataFrame.createOrReplaceTempView("validity");
		
		String window = "";
		String authors = "";
		String year = "";
		String journal = "";
		String title = "";
		ArrayList<Row> validityTemp = new ArrayList<Row>();
		
		for(Row r: validityDataFrame.collectAsList()) {
			//set the strings for each line of joined.dataset
			window = "";
		 	authors = "";
			year = "";
		 	journal = "";
		 	title = "";
		 	//get the elements needed
			window = r.getString(0);
			authors = r.getString(7).toLowerCase().trim();
			year = r.getString(6);
			journal = r.getString(5).toLowerCase().trim();
			 //now take and normalize the title
			title = (r.getString(4)).toLowerCase().replaceAll("[^a-zA-z0-9 ]","_").replaceAll(" +"," ");
								//to count the authors reflection on the match window
			double confidence = ((10*regexpcountdistance(authors,comprspaces(regexpr("\\b(\\w{1,2})\\b",regexpr(title,window," "),"Null"),"Null")))
								//to count the publisher's reflection on the match window
								+ 10*regexpcountdistance(journal,comprspaces(regexpr("\\b(\\w{1,2})\\b",regexpr(title,window," "),"Null"),"Null"))
								//to count the year's reflection on the match window
								+ 3*regexpcountdistance(year,comprspaces(regexpr("\\b(\\w{1,2})\\b",regexpr(title,window," "),"Null"),"Null")))
								/(10*calc((regexpcountwords("\\s",comprspaces(window,"Null")) - regexpcountwords("\\s",title)))) 
								//leipei i diairesi
								+((regexpcountwords("\\s",title)+1)*1.0/(regexpcountwords("\\s",comprspaces(window,"Null"))+1))*1.0/2 
								;
								
			validityTemp.add(RowFactory.create(window,r.getString(1),r.getString(2),confidence));
			//System.out.println(r);
			
		}
		//double confidence = (10*regexpcountdistance(authors,comprspaces(regexpr("\\b(\\w{1,2})\\b",regexpr(journal,window,""),"Null"),"Null")));
		//System.out.println(window);
		//System.out.println(authors);
		//System.out.println(year);
		//System.out.println(journal);
		//System.out.println(title);
		//System.out.println(confidence);
		
		Dataset<Row> validity_final = spark.createDataFrame(validityTemp, schema);
		validity_final.createOrReplaceTempView("validity");
		return "validity";
	}

}

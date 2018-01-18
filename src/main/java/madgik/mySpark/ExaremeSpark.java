package madgik.mySpark;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;

import madgik.mySpark.console.Console;
import madgik.mySpark.parser.ParserUtils;
import madgik.mySpark.parser.exception.VtExtensionParserException;
import madgik.mySpark.udaf.Joinstr;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.commons.lang3.StringUtils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
///

public class ExaremeSpark {
	
	public static void main(String[] args) {
			
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		LineReader reader = Console.initLineReader();
		
		while(true){
				
			try{
				
				ExaremeSparkSession spark =  ExaremeSparkSession.exaremebuild()
						.master("local[1]")
						.appName("ExaremeSpark")
						.getOrCreateExareme();   
				
				spark.getSparkSession().udf().register("Normtitles",Normtitles,DataTypes.StringType);
				spark.getSparkSession().udf().register("Comprspaces",Comprspaces,DataTypes.StringType);
				spark.getSparkSession().udf().register("Regexpcountwords",Regexpcountwords,DataTypes.IntegerType);
				spark.getSparkSession().udf().register("Regexpr",Regexpr,DataTypes.StringType);
				spark.getSparkSession().udf().register("Findsignal", Findsignal,DataTypes.IntegerType);
				spark.getSparkSession().udf().register("Regexpcountdistance",Regexpcountdistance,DataTypes.DoubleType);
				spark.getSparkSession().udf().register("Joinstr", new Joinstr());
				String query;
				try{
					query = reader.readLine(Console.ANSI_BOLD+Console.ANSI_BRIGHT_GREEN + "exaremeSQL> "+ Console.ANSI_RESET);
				}catch(UserInterruptException |EndOfFileException e){
					Console.printMessage(("\n"+Console.ANSI_BRIGHT_ORANGE+"Application is going to stop\n"+Console.ANSI_RESET));
					spark.getSparkSession().stop();
					break;
				}
				
				try{
					spark.sqlExtended(query).show(1000,false);;
				}catch(VtExtensionParserException e) {
					if(e.getMessage() != null)
						Console.printMessage(ParserUtils.displayError(e.getMessage()));
				}
					
			} catch (Exception e ){
				if(e.getCause() != null){
					Console.printMessage(ParserUtils.displayError(e.getCause().getMessage()));
				}else{
					Console.printMessage(ParserUtils.displayError(e.getMessage()));
				}
			}
		}
		
			
	}
	private static UDF1<String,String> Normtitles = new UDF1<String,String>(){
		public String call(final String str) throws Exception{
			return str.toLowerCase().replaceAll("[^a-zA-z0-9 ]","_").replaceAll(" +"," ");
		}
	};
	//select comprspaces(line,"NULL") from (select * from foo(file for foo to read to dataset containing only one field with value line)
	//second argument NULL for the needs of the excercise
	private static UDF2<String,String,String> Comprspaces = new UDF2<String,String,String>(){
		@Override
		public String call(String arg0, String arg1) throws Exception {
			if (arg1.equals("NULL"))
	            return arg0.trim().replaceAll(" +"," ");
	         
	         else
	            return arg0.trim().replaceAll(" +"," ") +" "+ arg1.trim().replaceAll(" +"," ");
		}
	};
	//select regexpcountwords('start',line) from (select * from foo(',','../demospaces.txt'))
	private static UDF2<String,String,Integer> Regexpcountwords = new UDF2<String,String,Integer>(){
		@Override
		public Integer call(String arg0, String arg1) throws Exception {
			 //arg0 ->  pattern
			 //arg1 -> expression
			 Pattern myPattern = Pattern.compile(arg0, Pattern.UNICODE_CHARACTER_CLASS);
             Matcher myMatcher = myPattern.matcher(arg1);
             int sum = 0;
             int occurences ;
             while (myMatcher.find()) {
                   occurences = 0; 
                   System.out.println(myMatcher.group());
                   occurences = StringUtils.countMatches(myMatcher.group().trim()," ") + 1;
                   sum = sum + occurences;
                   //sum = sum + myMatcher.group().trim().count(' ')+1;
             }
             return sum;
		}
	};
	//select regexpr('start\\\\s(\\\\w+)\\\\send',line,'NULL') from foo(',','../demospaces.txt')
	//select regexpr('\\\\W+',line,'nonword') from foo(',','../demospaces.txt')	 line = @#$%@$#% tobereplaced @#$%@#$%
	//select regexpr('\\\\w+).*?(\\\\w+)',line,'NULL') from foo(',','../demospaces.txt') line = one two three,exaremesql does not accep parentheses 
	
	private static UDF3<String,String,String,String> Regexpr = new UDF3<String,String,String,String>(){
		//escape character backslash needs 4 backslashes for spark to identify it
		@Override
		public String call(String pattern, String expression, String replace_expression) throws Exception {
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
               return "NULL";
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
                  return "NULL";
            }
            else                     //case we have three arguments
               return expression.replaceAll(pattern,replace_expression);
		}
		
	};
	
	private static UDF2<String,String,Double> Regexpcountdistance = new UDF2<String,String,Double>(){
		//for each group of matcher we add count the result of division of 1 with the length(in terms of words) of the expression
		//example : select regexpcountdistance('start',line) as match_score from (select * from foo(',','../demospaces.txt'))
		//output : 0.66 for line : start end start
		@Override
		public Double call(String pattern, String expression) throws Exception {
			   double count = 0.0;
	          
	           Pattern myPattern = Pattern.compile(pattern, Pattern.UNICODE_CHARACTER_CLASS);
	           Matcher myMatcher = myPattern.matcher(expression);
	           int exlength = expression.split(" ").length;
	           while (myMatcher.find()){
	               count = count + 1.0/exlength;
	           }

	           return count;
		}
		
	};
	//execute command example
	//select line,findsignal(line) as signal from(select * from readpaper('../demo.txt'))
	private static UDF1<String,Integer> Findsignal = new UDF1<String,Integer>(){

		@Override
		public Integer call(String l) throws Exception {
			 String pattern = "((.*)1[5-9]\\d{2,2}(.*))|((.*)20\\d{2,2}(.*))|((.*)[^A-Za-z0-9]et al[^A-Za-z0-9](.*))|((.*)http(.*))";  
			 Pattern r = Pattern.compile(pattern);
			 Matcher matcher = r.matcher(l);
			 int result = -1;
			 if (matcher.find())
					result = 1;
			 else
					result = 0;
		
			return result;
		}
		
	};
	
	
	
	
	

}
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
					spark.sqlExtended(query).show(100,false);;
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
	//second argument NULL for the needs of the execercise
	private static UDF2<String,String,String> Comprspaces = new UDF2<String,String,String>(){
		@Override
		public String call(String arg0, String arg1) throws Exception {
			if (arg1.equals("NULL"))
	            return arg0.trim().replaceAll(" +"," ");
	         
	         else
	            return arg0.trim().replaceAll(" +"," ") +" "+ arg1.trim().replaceAll(" +"," ");
		}
	};
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
                   // Do your stuff here
                   occurences = 0; 
                   System.out.println(myMatcher.group());
                   occurences = StringUtils.countMatches(myMatcher.group().trim()," ") + 1;
                   sum = sum + occurences;
                   //sum = sum + myMatcher.group().trim().count(' ')+1;
             }
             return sum;
		}
	};
	
	//select regexpcountwords('start',line) from (select * from foo(',','../demospaces.txt'))
	
	
	
	
	
	
	
	
	
	

}
package madgik.mySpark;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;

import madgik.mySpark.console.Console;
import madgik.mySpark.parser.ParserUtils;
import madgik.mySpark.parser.exception.VtExtensionParserException;

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
				
				/* textwindow2s registration */
//				spark.getSparkSession().udf().register("textwindow2s", new UDF4<String, Integer, Integer, Integer, ArrayList<ArrayList<String>>>(){
//					@Override
//					public ArrayList<ArrayList<String>> call(String s, Integer prev, Integer mid, Integer next){
//						String[] tokens = s.split(" ");
//						for(int i = 0; i < tokens.length-mid+1; i++){
//							int im = i+mid;
//							
//						}
//						return "hello";
//					}
//				}, DataTypes.StringType);
				
				String query;
				try{
					query = reader.readLine(Console.ANSI_BOLD+Console.ANSI_BRIGHT_GREEN + "exaremeSQL> "+ Console.ANSI_RESET);
				}catch(UserInterruptException |EndOfFileException e){
					Console.printMessage(("\n"+Console.ANSI_BRIGHT_ORANGE+"Application is going to stop\n"+Console.ANSI_RESET));
					spark.getSparkSession().stop();
					break;
				}
				
				try{
					spark.sqlExtended(query).show();;
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

}

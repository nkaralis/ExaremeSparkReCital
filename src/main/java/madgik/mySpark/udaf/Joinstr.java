package madgik.mySpark.udaf;

import java.util.ArrayList;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

public class Joinstr extends UserDefinedAggregateFunction {

	private StructType inputSchema;
	private StructType bufferSchema;
	
	public Joinstr() {
		ArrayList<StructField> inputFields = new ArrayList<StructField>();
	    inputFields.add(DataTypes.createStructField("lines", DataTypes.StringType, true));
	    inputSchema = DataTypes.createStructType(inputFields);

	    ArrayList<StructField> bufferFields = new ArrayList<>();
	    bufferFields.add(DataTypes.createStructField("ref_section", DataTypes.StringType, true));
	    bufferSchema = DataTypes.createStructType(bufferFields);
	}
	
	@Override
	public StructType inputSchema() {
		return inputSchema;
	}
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return DataTypes.StringType;
	}

	@Override
	public boolean deterministic() {
		return true;
	}
	
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String newStr = buffer1.getString(0) + buffer2.getString(0);
		buffer1.update(0, newStr);
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		String newStr;
		if(buffer.getString(0).length() == 0)
			newStr = buffer.getString(0) + input.getString(0);
		else
			newStr = buffer.getString(0) + " "+ input.getString(0);
		buffer.update(0, newStr);
	}
	
	@Override
	public Object evaluate(Row buffer) {
		return buffer.getString(0);
	}

}

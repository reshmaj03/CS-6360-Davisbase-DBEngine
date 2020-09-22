package dBEngine.data;

import static dBEngine.util.Constants.BIGINT;
import static dBEngine.util.Constants.DATE;
import static dBEngine.util.Constants.DATETIME;
import static dBEngine.util.Constants.DOUBLE;
import static dBEngine.util.Constants.EQUAL;
import static dBEngine.util.Constants.FLOAT;
import static dBEngine.util.Constants.GREATER_THAN;
import static dBEngine.util.Constants.GREATER_THAN_OR_EQUAL;
import static dBEngine.util.Constants.INT;
import static dBEngine.util.Constants.LESSER_THAN;
import static dBEngine.util.Constants.LESSER_THAN_OR_EQUAL;
import static dBEngine.util.Constants.NOT_EQUAL;
import static dBEngine.util.Constants.NULL;
import static dBEngine.util.Constants.SMALLINT;
import static dBEngine.util.Constants.TIME;
import static dBEngine.util.Constants.TINYINT;
import static dBEngine.util.Constants.YEAR;
import static dBEngine.util.DatatypeUtil.convertDateForWrite;
import static dBEngine.util.DatatypeUtil.convertDateTimeForWrite;
import static dBEngine.util.DatatypeUtil.convertYearForRead;

import dBEngine.util.Constants;

public class ColumnMetadata {
	public final String name;
	public final String datatype;
	public final byte ordinal_position;
	public byte code;
	public boolean isNullable = true;
	public boolean isUnique = false;
	public boolean isPrimary = false;
	public String value;
	public int length;

	public ColumnMetadata(String name, String datatype, byte pos) {
		this.name = name;
		this.datatype = datatype;
		ordinal_position = pos;
		code = getCode();
	}

	public ColumnMetadata createCopy() {
		ColumnMetadata newCol = new ColumnMetadata(name, datatype, ordinal_position);
		newCol.code = code;
		newCol.isNullable = isNullable;
		newCol.isPrimary = isPrimary;
		newCol.isUnique = isUnique;
		newCol.value = value;
		newCol.length = length;

		return newCol;
	}

	public void setNotNullable() {
		isNullable = false;
	}

	public void setUnique() {
		isUnique = true;
	}

	public void setPrimary() {
		isPrimary = true;
	}

	public byte getCode() {
		String dataty = datatype.toUpperCase();

		switch (dataty) {
		case "TINYINT":
		case "BYTE":
			length = 1;
			return TINYINT;
		case "SMALLINT":
		case "SHORT":
			length = 2;
			return SMALLINT;
		case "INT":
		case "INTEGER":
			length = 4;
			return INT;
		case "BIGINT":
		case "LONG":
			length = 8;
			return BIGINT;
		case "FLOAT":
			length = 4;
			return FLOAT;
		case "DOUBLE":
		case "REAL":
			length = 8;
			return DOUBLE;
		case "YEAR":
			length = 1;
			return YEAR;
		case "TIME":
			length = 4;
			return TIME;
		case "DATETIME":
			length = 8;
			return DATETIME;
		case "DATE":
			length = 8;
			return DATE;
		case "TEXT":
			length = 12;
			return Constants.TEXT;
		}

		return NULL;
	}

	public boolean evaluateCondition(Condition cond) {
		String dataty = datatype.toUpperCase();
		
		if (value == null || cond.value == null) {
			return false;
		}

		switch (dataty) {
		case "TINYINT":
		case "BYTE":
		case "SMALLINT":
		case "SHORT":
		case "INT":
		case "INTEGER":
		case "TIME":
			return compareIntValue(value, cond.operator, cond.value);
		case "BIGINT":
		case "LONG":
			return compareLongValue(value, cond.operator, cond.value);
		case "FLOAT":
			return compareFloatValue(value, cond.operator, cond.value);
		case "DOUBLE":
		case "REAL":
			return compareDoubleValue(value, cond.operator, cond.value);
		case "YEAR":
			return compareYearValue(value, cond.operator, cond.value);

		case "DATETIME":
			return compareDateTimeValue(value, cond.operator, cond.value);
		case "DATE":
			return compareDateValue(value, cond.operator, cond.value);
		case "TEXT":
			return compareTextValue(value, cond.operator, cond.value);
		}

		return false;
	}

	public boolean compareIntValue(String left, String comp, String right) {
		int leftInt = Integer.parseInt(left);
		int rightInt = Integer.parseInt(right);
		switch (comp) {
		case GREATER_THAN:
			return (leftInt > rightInt);
		case LESSER_THAN:
			return (leftInt < rightInt);
		case GREATER_THAN_OR_EQUAL:
			return (leftInt >= rightInt);
		case LESSER_THAN_OR_EQUAL:
			return (leftInt <= rightInt);
		case EQUAL:
			return (leftInt == rightInt);
		case NOT_EQUAL:
			return (leftInt != rightInt);
		}

		return false;

	}

	public boolean compareLongValue(String left, String comp, String right) {
		long leftLong = Long.parseLong(left);
		long rightLong = Long.parseLong(right);
		switch (comp) {
		case GREATER_THAN:
			return (leftLong > rightLong);
		case LESSER_THAN:
			return (leftLong < rightLong);
		case GREATER_THAN_OR_EQUAL:
			return (leftLong >= rightLong);
		case LESSER_THAN_OR_EQUAL:
			return (leftLong <= rightLong);
		case EQUAL:
			return (leftLong == rightLong);
		case NOT_EQUAL:
			return (leftLong != rightLong);
		}

		return false;

	}

	public boolean compareFloatValue(String left, String comp, String right) {
		float leftFloat = Float.parseFloat(left);
		float rightFloat = Float.parseFloat(right);
		switch (comp) {
		case GREATER_THAN:
			return (leftFloat > rightFloat);
		case LESSER_THAN:
			return (leftFloat < rightFloat);
		case GREATER_THAN_OR_EQUAL:
			return (leftFloat >= rightFloat);
		case LESSER_THAN_OR_EQUAL:
			return (leftFloat <= rightFloat);
		case EQUAL:
			return (leftFloat == rightFloat);
		case NOT_EQUAL:
			return (leftFloat != rightFloat);
		}

		return false;
	}

	public boolean compareDoubleValue(String left, String comp, String right) {
		double leftDouble = Double.parseDouble(left);
		double rightDouble = Double.parseDouble(right);
		switch (comp) {
		case GREATER_THAN:
			return (leftDouble > rightDouble);
		case LESSER_THAN:
			return (leftDouble < rightDouble);
		case GREATER_THAN_OR_EQUAL:
			return (leftDouble >= rightDouble);
		case LESSER_THAN_OR_EQUAL:
			return (leftDouble <= rightDouble);
		case EQUAL:
			return (leftDouble == rightDouble);
		case NOT_EQUAL:
			return (leftDouble != rightDouble);
		}

		return false;

	}

	public boolean compareYearValue(String left, String comp, String right) {
		int leftYear = Integer.valueOf(convertYearForRead(Integer.parseInt(left)));
		int rightYear = Integer.valueOf(convertYearForRead(Integer.parseInt(right)));
		switch (comp) {
		case GREATER_THAN:
			return (leftYear > rightYear);
		case LESSER_THAN:
			return (leftYear < rightYear);
		case GREATER_THAN_OR_EQUAL:
			return (leftYear >= rightYear);
		case LESSER_THAN_OR_EQUAL:
			return (leftYear <= rightYear);
		case EQUAL:
			return (leftYear == rightYear);
		case NOT_EQUAL:
			return (leftYear != rightYear);
		}
		return false;
	}

	public boolean compareDateTimeValue(String left, String comp, String right) {
		long leftLong = convertDateTimeForWrite(left);
		long rightLong = convertDateTimeForWrite(right);
		switch (comp) {
		case GREATER_THAN:
			return (leftLong > rightLong);
		case LESSER_THAN:
			return (leftLong < rightLong);
		case GREATER_THAN_OR_EQUAL:
			return (leftLong >= rightLong);
		case LESSER_THAN_OR_EQUAL:
			return (leftLong <= rightLong);
		case EQUAL:
			return (leftLong == rightLong);
		case NOT_EQUAL:
			return (leftLong != rightLong);
		}
		return false;
	}

	public boolean compareDateValue(String left, String comp, String right) {
		long leftLong = convertDateForWrite(left);
		long rightLong = convertDateForWrite(right);
		switch (comp) {
		case GREATER_THAN:
			return (leftLong > rightLong);
		case LESSER_THAN:
			return (leftLong < rightLong);
		case GREATER_THAN_OR_EQUAL:
			return (leftLong >= rightLong);
		case LESSER_THAN_OR_EQUAL:
			return (leftLong <= rightLong);
		case EQUAL:
			return (leftLong == rightLong);
		case NOT_EQUAL:
			return (leftLong != rightLong);
		}
		return false;
	}

	public boolean compareTextValue(String left, String comp, String right) {
		int compValue = left.compareTo(right);

		switch (comp) {
		case GREATER_THAN:
			return compValue > 0;
		case LESSER_THAN:
			return compValue < 0;
		case GREATER_THAN_OR_EQUAL:
			return compValue > 0;
		case LESSER_THAN_OR_EQUAL:
			return compValue <= 0;
		case EQUAL:
			return left.equals(right);
		case NOT_EQUAL:
			return !left.equals(right);
		}
		return false;
	}

}

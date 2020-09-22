package dBEngine.util;

public class Constants {
	public static final String PROMPT = "davisql> ";
	public static final String VERSION = "1.0";
	public static final String COPYRIGHT = "c©2020 Reshma Jalla";

	public static final int PAGE_SIZE_POWER = 9;
	public static final int PAGE_SIZE = (int) Math.pow(2, PAGE_SIZE_POWER);

	public static final String DAVISBASE_TABLES = new String("davisbase_tables");
	public static final String DAVISBASE_COLUMNS = new String("davisbase_columns");
	public static final String DAVISBASE_TABLES_PATH = new String("data/catalog/davisbase_tables.tbl");
	public static final String DAVISBASE_COLUMNS_PATH = new String("data/catalog/davisbase_columns.tbl");

	public static final String DAVISBASE_USER_DIR = new String("data/user_data/");
	public static final String DAVISBASE_CATALOG_DIR = new String("data/catalog/");

	public static final int LEAF_PAGE = 0x0D;
	public static final int INTERIOR_PAGE = 0x05;
	public static final int CELL_COUNT = 0x02;
	public static final int CONTENT_START = 0x04;
	public static final int RIGHT_PAGE = 0x06;
	public static final int PARENT_PAGE = 0x0A;
	public static final int CELL_START_POSITIONS = 0x10;

	public static final short CELL_HEADER_LENGTH = 6;
	public static final short INTERIOR_CELL_LENGTH = 8;

	public static final byte NULL = 0x00;
	public static final byte TINYINT = 0x01;
	public static final byte BYTE = 0x01;
	public static final byte SMALLINT = 0x02;
	public static final byte SHORT = 0x02;
	public static final byte INT = 0x03;
	public static final byte INTEGER = 0x03;
	public static final byte BIGINT = 0x04;
	public static final byte LONG = 0x04;
	public static final byte FLOAT = 0x05;
	public static final byte DOUBLE = 0x06;
	public static final byte REAL = 0x06;
	public static final byte YEAR = 0x08;
	public static final byte TIME = 0x09;
	public static final byte DATETIME = 0x0A;
	public static final byte DATE = 0x0B;
	public static final byte TEXT = 0x0C;

	public static final String PK = "PRIMARY KEY";
	public static final String PRIMARY = "PRIMARY";
	public static final String UNIQUE = "UNIQUE";
	public static final String NOT_NULL = "NOT NULL";
	public static final String NOT = "NOT";
	public static final String YES = "YES";
	public static final String NO = "NO";

	public static final String NULL_VALUE = "null";

	public static final String AND = "and";
	public static final String OR = "or";

	public static final String GREATER_THAN = ">";
	public static final String LESSER_THAN = "<";
	public static final String GREATER_THAN_OR_EQUAL = ">=";
	public static final String LESSER_THAN_OR_EQUAL = "<=";
	public static final String EQUAL = "=";
	public static final String NOT_EQUAL = "<>";

}

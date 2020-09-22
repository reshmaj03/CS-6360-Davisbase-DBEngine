package dBEngine.sdl;

import static dBEngine.sdl.CatalogColumns.initializeDavisbaseColumns;
import static dBEngine.sdl.CatalogColumns.insertNewColumn;
import static dBEngine.sdl.CatalogTables.initializeDavisbaseTables;
import static dBEngine.sdl.CatalogTables.insertNewTable;
import static dBEngine.util.Constants.DAVISBASE_COLUMNS;
import static dBEngine.util.Constants.DAVISBASE_TABLES;

import java.io.File;

public class DataStoreUtil {

	public static void initializeDataStore() {

		File dataDir = new File("data");
		if (dataDir.exists()) {
			return;
		}

		dataDir.mkdir();

		File catalogDir = new File("data/catalog");
		catalogDir.mkdir();

		File userDir = new File("data/user_data");
		userDir.mkdir();

		try {
			initializeDavisbaseTables();
			insertNewTable(DAVISBASE_TABLES);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}

		try {

			initializeDavisbaseColumns();
			insertNewTable(DAVISBASE_COLUMNS);
			insertNewColumn(DAVISBASE_TABLES, "Table_Name", "TEXT", 1, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_TABLES, "Root_Page", "SMALLINT", 2, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_TABLES, "Last_Id", "INT", 3, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_TABLES, "Record_Count", "INT", 4, "NO", "NO", "NO");

			insertNewColumn(DAVISBASE_COLUMNS, "Table_Name", "TEXT", 1, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_COLUMNS, "Column_Name", "TEXT", 2, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_COLUMNS, "Datatype", "TEXT", 3, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_COLUMNS, "OrdinalPosition", "TINYINT", 4, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_COLUMNS, "isNullable", "TEXT", 5, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_COLUMNS, "isUnique", "TEXT", 6, "NO", "NO", "NO");
			insertNewColumn(DAVISBASE_COLUMNS, "isPrimary", "TEXT", 7, "NO", "NO", "NO");

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}
}

package dBEngine.ddl;

import static dBEngine.sdl.CatalogColumns.removeColumns;
import static dBEngine.sdl.CatalogTables.removeTable;
import static dBEngine.util.Constants.DAVISBASE_USER_DIR;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class DropTable {

	public static void run(ArrayList<String> commandTokens) {

		if (!validateCommand(commandTokens)) {
			return;
		}
		String tableName = commandTokens.get(2);
		String tableFileName = tableName + ".tbl";

		try {
			File tableFile = new File(DAVISBASE_USER_DIR + tableFileName);
			tableFile.delete();

			removeColumns(tableName);
			removeTable(tableName);

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(e);
			System.out.println("Error occured during table deletion");
		}

	}

	private static boolean validateCommand(ArrayList<String> tokens) {
		if (!tokens.get(1).equalsIgnoreCase("table")) {
			System.out.println("Unrecognized command. Use help;");
			return false;
		}

		String fileName = tokens.get(2) + ".tbl";
		if (!(new File(DAVISBASE_USER_DIR + fileName)).exists()) {
			System.out.println("Error: Table does not exist");
			return false;
		}

		return true;
	}
}

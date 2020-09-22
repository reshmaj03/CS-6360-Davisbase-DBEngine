package dBEngine.ddl;

import static dBEngine.sdl.CatalogColumns.insertNewColumn;
import static dBEngine.sdl.CatalogTables.insertNewTable;
import static dBEngine.util.Constants.DAVISBASE_USER_DIR;
import static dBEngine.util.Constants.NO;
import static dBEngine.util.Constants.NOT;
import static dBEngine.util.Constants.NOT_NULL;
import static dBEngine.util.Constants.PK;
import static dBEngine.util.Constants.PRIMARY;
import static dBEngine.util.Constants.UNIQUE;
import static dBEngine.util.Constants.YES;
import static dBEngine.util.TableFileUtil.setNewTableFileHeader;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class CreateTable {
	public static void run(ArrayList<String> commandTokens) {

		if (!validateCommand(commandTokens)) {
			return;
		}

		int n = commandTokens.size();
		String tableName = commandTokens.get(2);
		String tableFileName = tableName + ".tbl";

		try {
			RandomAccessFile tableFile = new RandomAccessFile(DAVISBASE_USER_DIR + tableFileName, "rw");
			setNewTableFileHeader(tableFile);
			tableFile.close();
			insertNewTable(tableName);

			int ordinalPos = 0;
			int i = 4;
			while (i < n && !commandTokens.get(i).equalsIgnoreCase(")")) {
				ArrayList<String> column = new ArrayList<>();
				ordinalPos++;
				while (!commandTokens.get(i).equalsIgnoreCase(",") && !commandTokens.get(i).equalsIgnoreCase(")")) {
					column.add(commandTokens.get(i));
					i++;
				}

				String columnName = column.get(0);
				String datatype = column.get(1);
				String isNullable = YES;
				String isUnique = NO;
				String isPrimary = NO;
				int j = 2;
				while (j < column.size()) {
					String constraint = column.get(j);
					if (constraint.equalsIgnoreCase(PRIMARY)) {
						constraint = column.get(j) + " " + column.get(++j);
						if (constraint.equalsIgnoreCase(PK)) {
							isPrimary = YES;
						}
					} else if (constraint.equalsIgnoreCase(UNIQUE)) {
						isUnique = YES;
					} else if (constraint.equalsIgnoreCase(NOT)) {
						constraint = column.get(j) + " " + column.get(++j);
						if (constraint.equalsIgnoreCase(NOT_NULL)) {
							isNullable = NO;
						}
					}
					j++;
				}

				insertNewColumn(tableName, columnName, datatype, ordinalPos, isNullable, isUnique, isPrimary);

				if (commandTokens.get(i).equalsIgnoreCase(")")) {
					break;
				}

				i++;

			}

		} catch (Exception e) {
			System.out.println(e);
		}

	}

	private static boolean validateCommand(ArrayList<String> tokens) {
		if (!tokens.get(1).equalsIgnoreCase("table")) {
			System.out.println("Unrecognized command. Use help;");
			return false;
		}

		String fileName = tokens.get(2) + ".tbl";
		if ((new File(DAVISBASE_USER_DIR + fileName)).exists()) {
			System.out.println("Error: Table already exists");
			return false;
		}

		return true;
	}
}

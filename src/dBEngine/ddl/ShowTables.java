package dBEngine.ddl;

import static dBEngine.util.Constants.CELL_START_POSITIONS;
import static dBEngine.util.Constants.CONTENT_START;
import static dBEngine.util.Constants.DAVISBASE_TABLES_PATH;
import static dBEngine.util.Constants.PAGE_SIZE;
import static dBEngine.util.Constants.RIGHT_PAGE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class ShowTables {

	public static void run(ArrayList<String> commandTokens) {

		try {

			List<String> tableNames = fetchTables();
			displayResult(tableNames);

		} catch (Exception e) {
			System.out.println(e);
		}

	}

	public static List<String> fetchTables() throws IOException {
		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");
		List<String> tableNames = new ArrayList<>();

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tablesFile.seek(cellPointer);
			int contentStart = tablesFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			tablesFile.seek(pageSt);
			int pageContentStart = pageStart + tablesFile.readShort();

			while (contentStart != 0 && !(pageContentStart - cellPointer < 2)) {
				tablesFile.seek(pageStart + contentStart);
				int payloadSize = tablesFile.readShort();
				int row = tablesFile.readInt();
				int columns = tablesFile.readByte();
				long colPos = tablesFile.getFilePointer();

				int nameLength = tablesFile.readByte() - 12;
				tablesFile.seek(colPos + columns);
				byte[] nb = new byte[nameLength];
				tablesFile.read(nb);
				tableNames.add(new String(nb));

				cellPointer += 2;
				tablesFile.seek(cellPointer);
				contentStart = tablesFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tablesFile.seek(rightPagePos);
			page = tablesFile.readInt();
		}
		tablesFile.close();
		return tableNames;
	}

	public static void displayResult(List<String> tableNames) {
		System.out.println("table_name");
		System.out.println("-----------------");
		for (String s : tableNames) {
			System.out.println(s);
		}
	}

}

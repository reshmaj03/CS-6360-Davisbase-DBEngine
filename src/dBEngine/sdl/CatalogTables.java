package dBEngine.sdl;

import static dBEngine.data.BPlusTree.splitLeafPage;
import static dBEngine.util.Constants.CELL_START_POSITIONS;
import static dBEngine.util.Constants.CONTENT_START;
import static dBEngine.util.Constants.DAVISBASE_TABLES;
import static dBEngine.util.Constants.DAVISBASE_TABLES_PATH;
import static dBEngine.util.Constants.PAGE_SIZE;
import static dBEngine.util.Constants.RIGHT_PAGE;
import static dBEngine.util.TableFileUtil.setNewTableFileHeader;
import static dBEngine.util.TableFileUtil.updateHeaderAfterContentWrite;

import java.io.IOException;
import java.io.RandomAccessFile;

public class CatalogTables {

	static int rowId = 0;

	public static void initializeDavisbaseTables() throws IOException {
		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");
		setNewTableFileHeader(tablesFile);
		tablesFile.close();
	}

	public static void insertNewTable(String tableName) throws IOException {

		insertNewTable(tableName, 0, 0, 0);
	}

	public static void insertNewTable(String tableName, int rootPage, int lastId, int recordCount) throws IOException {

		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");

		long fileLength = tablesFile.length();
		int pages = (int) (fileLength / PAGE_SIZE);
		int pageNo = pages - 1;
		int pageStart = pageNo * PAGE_SIZE;

		short cellLength = 6;
		short columns = 4;
		cellLength += columns + 1;
		cellLength += tableName.length(); // table name
		cellLength += 2; // root_page
		cellLength += 4; // last_id
		cellLength += 4; // record_count

		tablesFile.seek(pageStart + CONTENT_START);
		int contentStart = tablesFile.readShort();

		tablesFile.seek(pageStart + CELL_START_POSITIONS);

		long currentEmptyHeaderPos = tablesFile.getFilePointer();
		while (tablesFile.readShort() != 0) {
			currentEmptyHeaderPos = tablesFile.getFilePointer();
		}

		if (rowId == 0) {
			fetchRowIdFromTableData();
		}

		if (contentStart - currentEmptyHeaderPos  + pageStart< cellLength) {
			pageNo = splitLeafPage(tablesFile, DAVISBASE_TABLES, pageNo, rowId);

			contentStart = PAGE_SIZE;
			pageStart = pageNo * PAGE_SIZE;
			currentEmptyHeaderPos = pageStart + CELL_START_POSITIONS;
		}

		int newContentStart = contentStart - cellLength;
		tablesFile.seek(pageStart + newContentStart);
		tablesFile.writeShort(cellLength - 6);
		tablesFile.writeInt(++rowId);

		// Record header
		tablesFile.write(columns);
		tablesFile.write(12 + tableName.length());
		tablesFile.write(0x02);
		tablesFile.write(0x03);
		tablesFile.write(0x03);

		// Record body
		tablesFile.writeBytes(tableName);
		tablesFile.writeShort(rootPage);
		tablesFile.writeInt(lastId);
		tablesFile.writeInt(recordCount);

		// Update page header
		updateHeaderAfterContentWrite(tablesFile, pageNo, newContentStart, currentEmptyHeaderPos);
		tablesFile.close();
		increaseRecordCountAndLastId(DAVISBASE_TABLES);
	}

	public static int fetchRowIdFromTableData(String table_name) throws IOException {
		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tablesFile.seek(cellPointer);
			int recordStart = tablesFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			tablesFile.seek(pageSt);
			int pageContentStart = pageStart + tablesFile.readShort();
			
			while (recordStart != 0 && !(pageContentStart - cellPointer < 2)) {
				tablesFile.seek(pageStart + recordStart);
				int payloadSize = tablesFile.readShort();
				int row = tablesFile.readInt();
				int columns = tablesFile.readByte();
				long colPos = tablesFile.getFilePointer();

				int nameLength = tablesFile.readByte() - 12;
				tablesFile.seek(colPos + columns);
				byte[] nb = new byte[nameLength];
				tablesFile.read(nb);
				String name = new String(nb);
				if (name.equalsIgnoreCase(table_name)) {
					long pos = tablesFile.getFilePointer() + 2;
					tablesFile.seek(pos);
					int rId = tablesFile.readInt();
					tablesFile.close();
					return rId;
				}
				cellPointer += 2;
				tablesFile.seek(cellPointer);
				recordStart = tablesFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tablesFile.seek(rightPagePos);
			page = tablesFile.readInt();
		}
		tablesFile.close();
		return 0;

	}

	private static void fetchRowIdFromTableData() throws IOException {
		rowId = fetchRowIdFromTableData(DAVISBASE_TABLES);

	}

	public static void updateRootPageInMetadata(String tableName, int rootPage) throws IOException {
		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int pageSt = pageStart + CONTENT_START;
			tablesFile.seek(pageSt);
			int pageContentStart = pageStart + tablesFile.readShort();

			int cellPointer = pageStart + CELL_START_POSITIONS;
			tablesFile.seek(cellPointer);
			int recordStart = tablesFile.readShort();

			while (recordStart != 0 && cellPointer != pageContentStart) {
				tablesFile.seek(pageStart + recordStart + 6);
				int columns = tablesFile.readByte();
				int nameLength = tablesFile.readByte() - 12;

				long namePos = tablesFile.getFilePointer() + columns - 1;
				byte[] nameBuff = new byte[nameLength];
				tablesFile.seek(namePos);
				tablesFile.read(nameBuff);
				String name = new String(nameBuff);
				if (name.equalsIgnoreCase(tableName)) {
					tablesFile.writeShort(rootPage);
					tablesFile.close();
					return;
				}
				cellPointer += 2;
				tablesFile.seek(cellPointer);
				recordStart = tablesFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tablesFile.seek(rightPagePos);
			page = tablesFile.readInt();
		}
		tablesFile.close();
	}

	public static void increaseRecordCountAndLastId(String tableName) throws IOException {
		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tablesFile.seek(cellPointer);
			int recordStart = tablesFile.readShort();

			while (recordStart != 0) {
				tablesFile.seek(pageStart + recordStart + 6);
				int columns = tablesFile.readByte();
				int nameLength = tablesFile.readByte() - 12;
				long namePos = tablesFile.getFilePointer() + columns - 1;
				byte[] nameBuff = new byte[nameLength];
				tablesFile.seek(namePos);
				tablesFile.read(nameBuff, 0, nameLength);
				String name = new String(nameBuff);
				if (name.equalsIgnoreCase(tableName)) {
					long lastIdPos = tablesFile.getFilePointer() + 2;
					tablesFile.seek(lastIdPos);
					int lastId = tablesFile.readInt();
					++lastId;
					tablesFile.seek(lastIdPos);
					tablesFile.writeInt(lastId);

					long recordCountPos = lastIdPos + 4;
					int records = tablesFile.readInt();
					tablesFile.seek(recordCountPos);
					++records;
					tablesFile.writeInt(records);

					tablesFile.close();
					return;
				}
				cellPointer += 2;
				tablesFile.seek(cellPointer);
				recordStart = tablesFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tablesFile.seek(rightPagePos);
			page = tablesFile.readInt();
		}
		tablesFile.close();
	}

	public static void reduceRecordCount(String tableName, int records) throws IOException {
		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");
		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tablesFile.seek(cellPointer);
			int recordStart = tablesFile.readShort();

			while (recordStart != 0) {
				tablesFile.seek(pageStart + recordStart + 6);
				int columns = tablesFile.readByte();
				int nameLength = tablesFile.readByte() - 12;

				long namePos = tablesFile.getFilePointer() + columns - 1;
				byte[] nameBuff = new byte[nameLength];
				tablesFile.seek(namePos);
				tablesFile.read(nameBuff, 0, nameLength);
				String name = new String(nameBuff);
				if (name.equalsIgnoreCase(tableName)) {
					long recordCountPos = tablesFile.getFilePointer() + 2 + 4;
					int recordNo = tablesFile.readInt();
					tablesFile.seek(recordCountPos);
					tablesFile.writeInt(recordNo - records);
					tablesFile.close();
					return;
				}
				cellPointer += 2;
				tablesFile.seek(cellPointer);
				recordStart = tablesFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tablesFile.seek(rightPagePos);
			page = tablesFile.readInt();
		}
		tablesFile.close();
	}

	public static void removeTable(String tableName) throws IOException {
		RandomAccessFile tablesFile = new RandomAccessFile(DAVISBASE_TABLES_PATH, "rw");

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tablesFile.seek(cellPointer);
			int recordStart = tablesFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			tablesFile.seek(pageSt);
			int pageContentStart = pageStart + tablesFile.readShort();

			while (recordStart != 0 && !(pageContentStart - cellPointer < 2)) {

				tablesFile.seek(pageStart + recordStart);
				int payloadSize = tablesFile.readShort();
				int row = tablesFile.readInt();
				int columns = tablesFile.readByte();
				long columnsPos = tablesFile.getFilePointer();
				int nameLength = tablesFile.readByte() - 12;
				long tableNamePos = columnsPos + columns;
				byte[] name = new byte[nameLength];
				tablesFile.seek(tableNamePos);
				tablesFile.read(name, 0, nameLength);
				if (tableName.equalsIgnoreCase(new String(name))) {
					int shiftCell = cellPointer;
					tablesFile.seek(shiftCell);
					int currentValue = tablesFile.readShort();

					tablesFile.seek(shiftCell + 2);
					int shiftValue = tablesFile.readShort();
					while (currentValue != 0) {
						tablesFile.seek(shiftCell);
						if (!(pageContentStart - shiftCell - 2 < 2)) {
							tablesFile.writeShort(shiftValue);
							currentValue = shiftValue;
						} else {
							tablesFile.writeShort(0);
							break;
						}

						shiftCell += 2;
						tablesFile.seek(shiftCell + 2);
						shiftValue = tablesFile.readShort();
					}
					tablesFile.close();
					reduceRecordCount(DAVISBASE_TABLES, 1);
					return;

				}
				cellPointer += 2;
				tablesFile.seek(cellPointer);
				recordStart = tablesFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tablesFile.seek(rightPagePos);
			page = tablesFile.readInt();
		}
		tablesFile.close();
	}
}

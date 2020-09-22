package dBEngine.sdl;

import static dBEngine.data.BPlusTree.splitLeafPage;
import static dBEngine.sdl.CatalogTables.increaseRecordCountAndLastId;
import static dBEngine.sdl.CatalogTables.reduceRecordCount;
import static dBEngine.util.Constants.CELL_HEADER_LENGTH;
import static dBEngine.util.Constants.CELL_START_POSITIONS;
import static dBEngine.util.Constants.CONTENT_START;
import static dBEngine.util.Constants.DAVISBASE_COLUMNS;
import static dBEngine.util.Constants.DAVISBASE_COLUMNS_PATH;
import static dBEngine.util.Constants.PAGE_SIZE;
import static dBEngine.util.Constants.RIGHT_PAGE;
import static dBEngine.util.TableFileUtil.updateHeaderAfterContentWrite;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dBEngine.data.ColumnMetadata;
import dBEngine.util.TableFileUtil;

public class CatalogColumns {
	static int rowId = 0;

	public static void initializeDavisbaseColumns() throws IOException {

		RandomAccessFile columnsFile = new RandomAccessFile(DAVISBASE_COLUMNS_PATH, "rw");

		TableFileUtil.setNewTableFileHeader(columnsFile);

		columnsFile.close();

	}

	public static void insertNewColumn(String tableName, String columnName, String dataType, int ordinalPos,
			String isNullable, String isUnique, String isPrimary) throws IOException {

		RandomAccessFile columnsFile = new RandomAccessFile(DAVISBASE_COLUMNS_PATH, "rw");

		long fileLength = columnsFile.length();
		int pages = (int) (fileLength / PAGE_SIZE);
		int pageNo = pages - 1;
		int pageStart = pageNo * PAGE_SIZE;

		int cellLength = CELL_HEADER_LENGTH;
		int columns = 7;
		cellLength += columns + 1;
		cellLength += tableName.length(); // table name
		cellLength += columnName.length(); // column name
		cellLength += dataType.length();
		cellLength += 1; // ordinal_pos
		cellLength += isNullable.length();
		cellLength += isUnique.length();
		cellLength += isPrimary.length();

		columnsFile.seek(pageStart + CONTENT_START);
		int contentStart = columnsFile.readShort();

		columnsFile.seek(pageStart + CELL_START_POSITIONS);

		long currentEmptyHeaderPos = columnsFile.getFilePointer();
		while (columnsFile.readShort() != 0) {
			currentEmptyHeaderPos = columnsFile.getFilePointer();
		}

		if (rowId == 0) {
			fetchRowIdFromTableData();
		}

		if (contentStart - currentEmptyHeaderPos + pageStart < cellLength) {
			pageNo = splitLeafPage(columnsFile, DAVISBASE_COLUMNS, pageNo, rowId);

			contentStart = PAGE_SIZE;
			pageStart = pageNo * PAGE_SIZE;
			currentEmptyHeaderPos = pageStart + CELL_START_POSITIONS;
		}

		int newContentStart = contentStart - cellLength;
		columnsFile.seek(pageStart + newContentStart);
		columnsFile.writeShort(cellLength - 6);
		columnsFile.writeInt(++rowId);

		// Record header
		columnsFile.write(columns);
		columnsFile.write(12 + tableName.length());
		columnsFile.write(12 + columnName.length());
		columnsFile.write(12 + dataType.length());
		columnsFile.write(0x01);
		columnsFile.write(12 + isNullable.length());
		columnsFile.write(12 + isUnique.length());
		columnsFile.write(12 + isPrimary.length());

		// Record Payload
		columnsFile.writeBytes(tableName);
		columnsFile.writeBytes(columnName);
		columnsFile.writeBytes(dataType);
		columnsFile.write(ordinalPos);
		columnsFile.writeBytes(isNullable);
		columnsFile.writeBytes(isUnique);
		columnsFile.writeBytes(isPrimary);

		// Update page header
		updateHeaderAfterContentWrite(columnsFile, pageNo, newContentStart, currentEmptyHeaderPos);

		increaseRecordCountAndLastId(DAVISBASE_COLUMNS);

		columnsFile.close();
	}

	private static void fetchRowIdFromTableData() throws IOException {
		rowId = CatalogTables.fetchRowIdFromTableData(DAVISBASE_COLUMNS);

	}

	public static Map<Integer, ColumnMetadata> readColumnMetadata(String tableName) throws IOException {
		Map<Integer, ColumnMetadata> columnMap = new HashMap<>();
		List<ColumnMetadata> columns = new ArrayList<>();
		RandomAccessFile columnsFile = new RandomAccessFile(DAVISBASE_COLUMNS_PATH, "rw");

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			columnsFile.seek(cellPointer);
			int cellStart = columnsFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			columnsFile.seek(pageSt);
			int pageContentStart = pageStart + columnsFile.readShort();

			while (cellStart != 0 && !(pageContentStart - cellPointer < 2)) {
				columnsFile.seek(pageStart + cellStart);
				int payloadSize = columnsFile.readShort();
				int row = columnsFile.readInt();
				int columnsCount = columnsFile.readByte();
				byte[] b = new byte[columnsCount];
				columnsFile.read(b, 0, columnsCount);
				int contentSize = payloadSize - columnsCount - 1;
				byte[] cont = new byte[contentSize];
				columnsFile.read(cont, 0, contentSize);

				int tnLength = b[0] - 12;
				int offset = 0;
				String table = new String(cont, offset, tnLength);
				if (table.equalsIgnoreCase(tableName)) {
					offset += tnLength;
					int cnLength = b[1] - 12;
					String columnName = new String(cont, offset, cnLength);
					offset += cnLength;
					int dtLength = b[2] - 12;
					String datatype = new String(cont, offset, dtLength);
					offset += dtLength;
					byte pos = cont[offset];
					int ordPos = pos;
					offset += 1;
					ColumnMetadata column = new ColumnMetadata(columnName, datatype, pos);

					int nLength = b[4] - 12;
					String isNullable = new String(cont, offset, nLength);
					if (isNullable.equalsIgnoreCase("NO")) {
						column.setNotNullable();
					}
					offset += nLength;

					int uLength = b[5] - 12;
					String isUnique = new String(cont, offset, uLength);
					if (isUnique.equalsIgnoreCase("YES")) {
						column.setUnique();
					}
					offset += uLength;

					int pLength = b[6] - 12;
					String isPrimary = new String(cont, offset, pLength);
					if (isPrimary.equalsIgnoreCase("YES")) {
						column.setPrimary();
					}
					offset += pLength;
					columns.add(column);
					columnMap.put(ordPos, column);
				}
				cellPointer += 2;
				columnsFile.seek(cellPointer);
				cellStart = columnsFile.readShort();
			}
			int rightPagePos = pageStart + RIGHT_PAGE;
			columnsFile.seek(rightPagePos);
			page = columnsFile.readInt();
		}

		columnsFile.close();
		return columnMap;

	}

	public static void removeColumns(String tableName) throws IOException {

		RandomAccessFile columnsFile = new RandomAccessFile(DAVISBASE_COLUMNS_PATH, "rw");
		int deletedRecords = 0;

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			columnsFile.seek(cellPointer);
			int recordStart = columnsFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			columnsFile.seek(pageSt);
			int pageContentStart = pageStart + columnsFile.readShort();

			while (recordStart != 0 && !(pageContentStart - cellPointer < 2)) {

				columnsFile.seek(pageStart + recordStart);
				int payloadSize = columnsFile.readShort();
				int row = columnsFile.readInt();
				int columns = columnsFile.readByte();
				long columnsPos = columnsFile.getFilePointer();
				int nameLength = columnsFile.readByte() - 12;
				long tableNamePos = columnsPos + columns;
				byte[] name = new byte[nameLength];
				columnsFile.seek(tableNamePos);
				columnsFile.read(name, 0, nameLength);
				if (tableName.equalsIgnoreCase(new String(name))) {
					int shiftCell = cellPointer;
					columnsFile.seek(shiftCell);
					int currentValue = columnsFile.readShort();

					columnsFile.seek(shiftCell + 2);
					int shiftValue = columnsFile.readShort();
					while (currentValue != 0) {
						columnsFile.seek(shiftCell);
						if (!(pageContentStart - shiftCell - 2 < 2)) {
							columnsFile.writeShort(shiftValue);
							currentValue = shiftValue;
						} else {
							columnsFile.writeShort(0);
							break;
						}

						shiftCell += 2;
						columnsFile.seek(shiftCell + 2);
						shiftValue = columnsFile.readShort();
					}
					deletedRecords++;
					cellPointer -= 2;
				}
				cellPointer += 2;
				columnsFile.seek(cellPointer);
				recordStart = columnsFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			columnsFile.seek(rightPagePos);
			page = columnsFile.readInt();
		}

		columnsFile.close();
		reduceRecordCount(DAVISBASE_COLUMNS, deletedRecords);
	}
}

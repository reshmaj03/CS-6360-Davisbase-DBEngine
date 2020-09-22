package dBEngine.dml;

import static dBEngine.sdl.CatalogColumns.readColumnMetadata;
import static dBEngine.sdl.CatalogTables.reduceRecordCount;
import static dBEngine.util.Constants.AND;
import static dBEngine.util.Constants.BIGINT;
import static dBEngine.util.Constants.CELL_START_POSITIONS;
import static dBEngine.util.Constants.CONTENT_START;
import static dBEngine.util.Constants.DATE;
import static dBEngine.util.Constants.DATETIME;
import static dBEngine.util.Constants.DAVISBASE_USER_DIR;
import static dBEngine.util.Constants.DOUBLE;
import static dBEngine.util.Constants.FLOAT;
import static dBEngine.util.Constants.INT;
import static dBEngine.util.Constants.NULL;
import static dBEngine.util.Constants.PAGE_SIZE;
import static dBEngine.util.Constants.RIGHT_PAGE;
import static dBEngine.util.Constants.SMALLINT;
import static dBEngine.util.Constants.TIME;
import static dBEngine.util.Constants.TINYINT;
import static dBEngine.util.Constants.YEAR;
import static dBEngine.util.DatatypeUtil.convertDateForRead;
import static dBEngine.util.DatatypeUtil.convertDateTimeForRead;
import static dBEngine.util.DatatypeUtil.convertYearForRead;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dBEngine.data.ColumnMetadata;
import dBEngine.data.Condition;

public class DeleteFrom {

	public static void run(ArrayList<String> commandTokens) {
		String tableName = commandTokens.get(2);

		try {
			RandomAccessFile tableFile = new RandomAccessFile(DAVISBASE_USER_DIR + tableName + ".tbl", "rw");

			Map<Integer, ColumnMetadata> metadataMap = readColumnMetadata(tableName);

			int deletedRecords = 0;

			if (commandTokens.size() == 3) {
				deletedRecords = removeTableData(tableFile);
			} else {
				String conditionsOp = new String();
				Map<Integer, Condition> condMap = new HashMap<>();
				int j = 1;
				int i = 4;
				Condition cond = new Condition();
				String colName = commandTokens.get(i);
				cond.ordPos = metadataMap.values().stream().filter(c -> c.name.equalsIgnoreCase(colName)).findFirst()
						.get().ordinal_position;
				++i;
				cond.operator = commandTokens.get(i);
				++i;
				cond.value = commandTokens.get(i);
				condMap.put(j, cond);
				i++;
				if (i < commandTokens.size()) {
					conditionsOp = commandTokens.get(i);
					i++;
					Condition cond1 = new Condition();
					String colName1 = commandTokens.get(i);
					cond1.ordPos = metadataMap.values().stream().filter(c -> c.name.equalsIgnoreCase(colName1))
							.findFirst().get().ordinal_position;
					++i;
					cond1.operator = commandTokens.get(i);
					++i;
					cond1.value = commandTokens.get(i);
					condMap.put(++j, cond1);
					i++;
				}

				deletedRecords = deleteData(tableFile, metadataMap, condMap, conditionsOp);

			}

			if (deletedRecords != 0) {
				reduceRecordCount(tableName, deletedRecords);
			}

			System.out.println("Deleted " + deletedRecords + " records");

			tableFile.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}
	}

	public static int deleteData(RandomAccessFile tableFile, Map<Integer, ColumnMetadata> metadata,
			Map<Integer, Condition> condMap, String condOp) throws IOException {
		int deletedRecords = 0;

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tableFile.seek(cellPointer);
			int recordStart = tableFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			tableFile.seek(pageSt);
			int pageContentStart = pageStart + tableFile.readShort();

			while (recordStart != 0 && !(pageContentStart - cellPointer < 2)) {
				Map<Integer, ColumnMetadata> colData = getNewColumnMap(metadata);
				List<Boolean> evals = new ArrayList<>();
				tableFile.seek(pageStart + recordStart);
				int payloadSize = tableFile.readShort();
				int row = tableFile.readInt();
				int columns = tableFile.readByte();
				long colPos = tableFile.getFilePointer();
				long valuePos = colPos + columns;

				int i = 0;
				while (i < columns) {
					tableFile.seek(colPos + i);
					byte code = tableFile.readByte();
					ColumnMetadata col = colData.get(i + 1);
					tableFile.seek(valuePos);
					String val = null;
					if (code > 12) {
						int length = code - 12;
						byte[] b = new byte[length];
						tableFile.read(b);
						val = new String(b);
						col.length = length;
						col.code += length;
					} else if (code == NULL) {
						val = null;
						col.length = 0;
						col.code = NULL;
					} else if (code == TINYINT) {
						int b = tableFile.readByte();
						val = String.valueOf(b);
					} else if (code == SMALLINT) {
						int b = tableFile.readShort();
						val = String.valueOf(b);
					} else if (code == INT) {
						int b = tableFile.readInt();
						val = String.valueOf(b);
					} else if (code == BIGINT) {
						long b = tableFile.readLong();
						val = String.valueOf(b);
					} else if (code == FLOAT) {
						float b = tableFile.readFloat();
						val = String.valueOf(b);
					} else if (code == DOUBLE) {
						double b = tableFile.readDouble();
						val = String.valueOf(b);
					} else if (code == YEAR) {
						int b = tableFile.readByte();
						val = String.valueOf(convertYearForRead(b));
					} else if (code == TIME) {
						int b = tableFile.readInt();
						val = String.valueOf(b);
					} else if (code == DATETIME) {
						long b = tableFile.readLong();
						val = String.valueOf(convertDateTimeForRead(b));
					} else if (code == DATE) {
						long b = tableFile.readLong();
						val = String.valueOf(convertDateForRead(b));
					}

					col.value = val;
					valuePos = tableFile.getFilePointer();

					Condition cond1 = condMap.get(1);
					if (cond1 != null && cond1.ordPos == i + 1) {
						evals.add(col.evaluateCondition(cond1));
					}
					Condition cond2 = condMap.get(2);
					if (cond2 != null && cond2.ordPos == i + 1) {
						evals.add(col.evaluateCondition(cond2));
					}
					i++;
				}

				boolean eval = false;
				if (condMap.size() == 2) {
					if (condOp.equalsIgnoreCase(AND)) {
						eval = evals.get(0) && evals.get(1);
					} else {
						eval = evals.get(0) || evals.get(1);
					}
				} else {
					eval = evals.get(0);
				}

				if (eval) {
					int shiftCell = cellPointer;
					tableFile.seek(shiftCell);
					int currentValue = tableFile.readShort();

					tableFile.seek(shiftCell + 2);
					int shiftValue = tableFile.readShort();
					while (currentValue != 0) {
						tableFile.seek(shiftCell);
						if (!(pageContentStart - shiftCell - 2 < 2)) {
							tableFile.writeShort(shiftValue);
							currentValue = shiftValue;
						} else {
							tableFile.writeShort(0);
							break;
						}

						shiftCell += 2;
						tableFile.seek(shiftCell + 2);
						shiftValue = tableFile.readShort();
					}
					deletedRecords++;
					cellPointer -= 2;
				}

				cellPointer += 2;
				tableFile.seek(cellPointer);
				recordStart = tableFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tableFile.seek(rightPagePos);
			page = tableFile.readInt();
		}
		return deletedRecords;
	}

	public static Map<Integer, ColumnMetadata> getNewColumnMap(Map<Integer, ColumnMetadata> metadata) {
		Map<Integer, ColumnMetadata> newMetadata = new HashMap<>();
		int i = 0;
		while (i < metadata.size()) {
			ColumnMetadata newCol = metadata.get(i + 1).createCopy();
			newMetadata.put(i + 1, newCol);
			i++;
		}
		return newMetadata;

	}

	public static int removeTableData(RandomAccessFile tablesFile) throws IOException {
		int deletedRecords = 0;
		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tablesFile.seek(cellPointer);
			int nextContent = tablesFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			tablesFile.seek(pageSt);
			int pageContentStart = pageStart + tablesFile.readShort();

			while (nextContent != 0 && !(pageContentStart - cellPointer < 2)) {
				tablesFile.seek(cellPointer);
				tablesFile.writeShort(0);
				nextContent = tablesFile.readShort();
				deletedRecords++;
				cellPointer += 2;
			}

			int rightPagePos = page * PAGE_SIZE + RIGHT_PAGE;
			tablesFile.seek(rightPagePos);
			page = tablesFile.readInt();
		}

		return deletedRecords;
	}
}

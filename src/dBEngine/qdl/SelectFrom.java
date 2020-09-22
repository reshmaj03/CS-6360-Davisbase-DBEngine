package dBEngine.qdl;

import static dBEngine.sdl.CatalogColumns.readColumnMetadata;
import static dBEngine.util.Constants.AND;
import static dBEngine.util.Constants.BIGINT;
import static dBEngine.util.Constants.CELL_START_POSITIONS;
import static dBEngine.util.Constants.CONTENT_START;
import static dBEngine.util.Constants.DATE;
import static dBEngine.util.Constants.DATETIME;
import static dBEngine.util.Constants.DAVISBASE_CATALOG_DIR;
import static dBEngine.util.Constants.DAVISBASE_COLUMNS;
import static dBEngine.util.Constants.DAVISBASE_TABLES;
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
import static dBEngine.util.PrintUtil.fixFormat;
import static dBEngine.util.PrintUtil.line;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import dBEngine.data.ColumnMetadata;
import dBEngine.data.Condition;

public class SelectFrom {
	public static void run(ArrayList<String> commandTokens) {
		int i = 0;
		int whereInd = 0;
		int fromInd = 0;
		boolean noConditions = true;
		boolean allColumns = true;
		if (!commandTokens.get(1).equalsIgnoreCase("*")) {
			allColumns = false;
		}

		String conditionsOp = new String();
		while (i < commandTokens.size()) {
			if (commandTokens.get(i).equalsIgnoreCase("from")) {
				fromInd = i;
			}
			if (commandTokens.get(i).equalsIgnoreCase("where")) {
				whereInd = i;
				noConditions = false;
			}
			i++;
		}

		String tableName = commandTokens.get(fromInd + 1);

		try {

			Map<Integer, ColumnMetadata> metadataMap = readColumnMetadata(tableName);
			Map<Integer, Condition> condMap = new HashMap<>();

			if (!noConditions) {
				i = whereInd + 1;
				int j = 1;
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
			}

			List<Integer> ordinalPositions = new ArrayList<>();
			if (!allColumns) {
				i = 1;
				while (i < fromInd) {
					if (commandTokens.get(i).equalsIgnoreCase(",")) {
						i++;
						continue;
					}
					String colName = commandTokens.get(i);
					int ordPos = metadataMap.values().stream().filter(c -> c.name.equalsIgnoreCase(colName)).findFirst()
							.get().ordinal_position;

					ordinalPositions.add(ordPos);
					i++;
				}

			}
			String path = DAVISBASE_USER_DIR;

			if (tableName.equalsIgnoreCase(DAVISBASE_TABLES) || tableName.equalsIgnoreCase(DAVISBASE_COLUMNS)) {
				path = DAVISBASE_CATALOG_DIR;

			}
			RandomAccessFile tableFile = new RandomAccessFile(path + tableName + ".tbl", "rw");

			Map<Integer, Map<Integer, ColumnMetadata>> data = new HashMap<>();
			Map<Integer, List<String>> dataStr;
			if (noConditions) {
				dataStr = readTableData(tableFile, metadataMap, data);
			} else {
				dataStr = readTableDataWithConditions(tableFile, metadataMap, data, condMap, conditionsOp);
			}

			displayResult(dataStr, metadataMap, ordinalPositions);

			tableFile.close();

		} catch (

		Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}
	}

	public static Map<Integer, List<String>> readTableData(RandomAccessFile tableFile,
			Map<Integer, ColumnMetadata> metadata, Map<Integer, Map<Integer, ColumnMetadata>> data) throws IOException {
		Map<Integer, List<String>> dataStr = new HashMap<>();

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tableFile.seek(cellPointer);
			int contentStart = tableFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			tableFile.seek(pageSt);
			int pageContentStart = pageStart + tableFile.readShort();

			while (contentStart != 0 && !(pageContentStart - cellPointer < 2)) {
				Map<Integer, ColumnMetadata> colData = getNewColumnMap(metadata);
				List<String> dataSet = new ArrayList<>();
				tableFile.seek(pageStart + contentStart);
				int payloadSize = tableFile.readShort();
				int row = tableFile.readInt();
				int columns = tableFile.readByte();
				long colPos = tableFile.getFilePointer();
				long valuePos = colPos + columns;
				String val = null;

				int i = 0;
				while (i < columns) {
					tableFile.seek(colPos + i);
					byte code = tableFile.readByte();
					ColumnMetadata col = colData.get(i + 1);
					tableFile.seek(valuePos);
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

					dataSet.add(val);
					col.value = val;
					valuePos = tableFile.getFilePointer();
					i++;
				}

				dataStr.put(row, dataSet);
				data.put(row, colData);

				cellPointer += 2;
				tableFile.seek(cellPointer);
				contentStart = tableFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tableFile.seek(rightPagePos);
			page = tableFile.readInt();
		}
		return dataStr;
	}

	public static Map<Integer, List<String>> readTableDataWithConditions(RandomAccessFile tableFile,
			Map<Integer, ColumnMetadata> metadata, Map<Integer, Map<Integer, ColumnMetadata>> data,
			Map<Integer, Condition> condMap, String condOp) throws IOException {
		Map<Integer, List<String>> dataStr = new HashMap<>();

		int page = 0;
		while (page != 0xFFFFFFFF) {
			int pageStart = page * PAGE_SIZE;
			int cellPointer = pageStart + CELL_START_POSITIONS;
			tableFile.seek(cellPointer);
			int contentStart = tableFile.readShort();

			int pageSt = pageStart + CONTENT_START;
			tableFile.seek(pageSt);
			int pageContentStart = pageStart + tableFile.readShort();

			while (contentStart != 0 && !(pageContentStart - cellPointer < 2)) {
				Map<Integer, ColumnMetadata> colData = getNewColumnMap(metadata);
				List<String> dataSet = new ArrayList<>();
				List<Boolean> evals = new ArrayList<>();
				tableFile.seek(pageStart + contentStart);
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

					dataSet.add(val);
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

				if (condMap.size() == 2) {
					boolean eval = false;
					if (condOp.equalsIgnoreCase(AND)) {
						eval = evals.get(0) && evals.get(1);
					} else {
						eval = evals.get(0) || evals.get(1);
					}

					if (eval) {
						dataStr.put(row, dataSet);
						data.put(row, colData);
					}
				} else {
					if (evals.get(0)) {
						dataStr.put(row, dataSet);
						data.put(row, colData);
					}
				}

				cellPointer += 2;
				tableFile.seek(cellPointer);
				contentStart = tableFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tableFile.seek(rightPagePos);
			page = tableFile.readInt();
		}
		return dataStr;
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

	public static void displayResult(Map<Integer, List<String>> resultSet, Map<Integer, ColumnMetadata> metadataMap,
			List<Integer> ordinalPositions) {

		List<ColumnMetadata> values = new ArrayList<>(metadataMap.values());
		String[] colName = new String[metadataMap.size()];
		int[] format = new int[metadataMap.size()];
		int c = 0;
		Iterator<ColumnMetadata> iter = values.iterator();
		while (iter.hasNext()) {
			ColumnMetadata col = iter.next();
			colName[c] = col.name;
			c++;

		}

		for (int i = 0; i < format.length; i++) {
			format[i] = colName[i].length();
		}

		for (List<String> str : resultSet.values()) {
			for (int j = 0; j < str.size(); j++) {
				if (str.get(j) == null)
					continue;
				if (format[j] < str.get(j).length()) {
					format[j] = str.get(j).length();
				}
			}
		}

		if (ordinalPositions.size() == 0) {

			for (int l : format)
				System.out.print(line("-", l + 3));

			System.out.println();

			for (int i = 0; i < colName.length; i++)
				System.out.print(fixFormat(format[i], colName[i]) + "|");

			System.out.println();

			for (int l : format)
				System.out.print(line("-", l + 3));

			System.out.println();

			for (List<String> str : resultSet.values()) {
				for (int j = 0; j < str.size(); j++)
					System.out.print(fixFormat(format[j], str.get(j)) + "|");
				System.out.println();
			}

		} else {
			int len = ordinalPositions.size();
			int[] dispColumns = new int[len];
			for (int j = 0; j < len; j++) {
				dispColumns[j] = ordinalPositions.get(j) - 1;
			}

			for (int j = 0; j < dispColumns.length; j++)
				System.out.print(line("-", format[dispColumns[j]] + 3));

			System.out.println();

			for (int j = 0; j < dispColumns.length; j++)
				System.out.print(fixFormat(format[dispColumns[j]], colName[dispColumns[j]]) + "|");

			System.out.println();

			for (int j = 0; j < dispColumns.length; j++)
				System.out.print(line("-", format[dispColumns[j]] + 3));

			System.out.println();

			for (List<String> str : resultSet.values()) {
				for (int j = 0; j < dispColumns.length; j++)
					System.out.print(fixFormat(format[dispColumns[j]], str.get(dispColumns[j])) + "|");
				System.out.println();
			}
			System.out.println();
		}

	}
}

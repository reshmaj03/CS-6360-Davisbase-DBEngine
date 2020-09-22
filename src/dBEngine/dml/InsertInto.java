package dBEngine.dml;

import static dBEngine.data.BPlusTree.splitLeafPage;
import static dBEngine.sdl.CatalogColumns.readColumnMetadata;
import static dBEngine.sdl.CatalogTables.fetchRowIdFromTableData;
import static dBEngine.sdl.CatalogTables.increaseRecordCountAndLastId;
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
import static dBEngine.util.Constants.NULL_VALUE;
import static dBEngine.util.Constants.PAGE_SIZE;
import static dBEngine.util.Constants.RIGHT_PAGE;
import static dBEngine.util.Constants.SMALLINT;
import static dBEngine.util.Constants.TIME;
import static dBEngine.util.Constants.TINYINT;
import static dBEngine.util.Constants.YEAR;
import static dBEngine.util.DatatypeUtil.convertDateForWrite;
import static dBEngine.util.DatatypeUtil.convertDateTimeForWrite;
import static dBEngine.util.DatatypeUtil.convertYearForWrite;
import static dBEngine.util.TableFileUtil.updateHeaderAfterContentWrite;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dBEngine.data.ColumnMetadata;

public class InsertInto {
	public static void run(ArrayList<String> commandTokens) {
		String tableName = commandTokens.get(2);

		try {
			RandomAccessFile tableFile = new RandomAccessFile(DAVISBASE_USER_DIR + tableName + ".tbl", "rw");

			Map<Integer, ColumnMetadata> metadataMap = readColumnMetadata(tableName);
			List<ColumnMetadata> metadata = new ArrayList<>(metadataMap.values());

			if (commandTokens.get(3).equalsIgnoreCase("values")) {
				int j = 5;
				int i = 0;
				while (!commandTokens.get(j).equalsIgnoreCase(")")) {
					if (commandTokens.get(j).equalsIgnoreCase(",")) {
						j++;
						continue;
					}
					String value = commandTokens.get(j);
					if (value.startsWith("\"")) {
						value = value.substring(1);
					}
					while (!commandTokens.get(j + 1).equalsIgnoreCase(",")
							&& !commandTokens.get(j + 1).equalsIgnoreCase(")")) {
						value = value + " " + commandTokens.get(++j);
					}
					if (value.endsWith("\"")) {
						value = value.substring(0, value.length() - 1);
					}
					ColumnMetadata colu = metadata.get(i);
					colu.value = value;
					if (colu.datatype.equalsIgnoreCase("text") && !value.equalsIgnoreCase(NULL_VALUE)) {
						colu.length = value.length();
						colu.code += (byte) value.length();
					}
					if (!colu.isNullable && value.equalsIgnoreCase(NULL_VALUE)) {
						System.out.println("ERROR: Not null constraint violated for column " + colu.name);
						return;
					}

					if (colu.isPrimary && value.equalsIgnoreCase(NULL_VALUE)) {
						System.out.println("ERROR: Primary key constraint violated for column " + colu.name);
						return;
					}

					if (value.equalsIgnoreCase(NULL_VALUE)) {
						colu.value = null;
						colu.code = NULL;
						colu.length = 0;
					}

					if (colu.isPrimary) {
						if (isConstraintViolated(tableFile, metadataMap, colu.ordinal_position, colu.value)) {
							System.out.println("ERROR: Primary key Constraint violated for column " + colu.name);
							return;
						}
					}
					if (colu.isUnique) {
						if (isConstraintViolated(tableFile, metadataMap, colu.ordinal_position, colu.value)) {
							System.out.println("ERROR: Unique Constraint violated for column " + colu.name);
							return;
						}
					}

					i++;
					j++;
				}
			} else {
				int i = 4;
				while (!commandTokens.get(i).equalsIgnoreCase(")")) {
					i++;
				}
				int columnStart = 4;
				int columnEnd = i;
				int valueStart = columnEnd + 3;
				i = valueStart;
				while (!commandTokens.get(i).equalsIgnoreCase(")")) {
					i++;
				}
				int valueEnd = i;

				int c = columnStart;
				int v = valueStart;
				int colCount = 0;
				List<Integer> colPositions = new ArrayList<>();
				while (v < valueEnd) {
					if (commandTokens.get(c).equalsIgnoreCase(",")) {
						c++;
						v++;
						continue;
					}

					String colName = commandTokens.get(c);
					ColumnMetadata col = metadata.stream().filter(m -> m.name.equalsIgnoreCase(colName)).findFirst()
							.get();

					String value = commandTokens.get(v);
					if (value.startsWith("\"")) {
						value = value.substring(1);
					}
					while (!commandTokens.get(v + 1).equalsIgnoreCase(",")
							&& !commandTokens.get(v + 1).equalsIgnoreCase(")")) {
						value = value + " " + commandTokens.get(++v);
					}
					if (value.endsWith("\"")) {
						value = value.substring(0, value.length() - 1);
					}

					col.value = value;
					if (col.datatype.equalsIgnoreCase("text") && !value.equalsIgnoreCase(NULL_VALUE)) {
						col.length = value.length();
						col.code += (byte) value.length();
					}

					if (!col.isNullable && value.equalsIgnoreCase(NULL_VALUE)) {
						System.out.println("ERROR: Not null constraint violated for column " + col.name);
						return;
					}

					if (col.isPrimary && value.equalsIgnoreCase(NULL_VALUE)) {
						System.out.println("ERROR: Primary key constraint violated for column " + col.name);
						return;
					}

					if (value.equalsIgnoreCase(NULL_VALUE)) {
						col.code = NULL;
						col.length = 0;
						col.value = null;
					}

					if (col.isPrimary) {
						if (isConstraintViolated(tableFile, metadataMap, col.ordinal_position, col.value)) {
							System.out.println("ERROR: Primary key Constraint violated for column " + col.name);
							return;
						}
					}
					if (col.isUnique) {
						if (isConstraintViolated(tableFile, metadataMap, col.ordinal_position, col.value)) {
							System.out.println("ERROR: Unique Constraint violated for column " + col.name);
							return;
						}
					}

					int ordPos = col.ordinal_position;
					colPositions.add(ordPos);

					c++;
					v++;
					colCount++;
				}

				if (colCount != metadata.size()) {
					i = 0;
					while (i < metadata.size()) {
						ColumnMetadata col = metadata.get(i);
						if (colPositions.contains(i + 1)) {
							i++;
							continue;
						}
						if (col.value == null && !col.isNullable) {
							System.out.println("ERROR: Not null constraint violated for column " + col.name);
							return;
						}
						if (col.value == null && col.isPrimary) {
							System.out.println("ERROR: Primary key constraint violated for column " + col.name);
							return;
						}
						if (col.value == null && col.isUnique) {
							if (isConstraintViolated(tableFile, metadataMap, i + 1, col.value)) {
								System.out.println("ERROR: Unique constraint violated for column " + col.name);
								return;
							}
						}
						if (col.value == null) {
							col.code = NULL;
							col.length = 0;
						}
						i++;
					}

				}
			}

			int cellLength = 6;
			int columnNo = metadata.size();
			cellLength += columnNo + 1;
			byte[] recHeader = new byte[columnNo + 1];
			recHeader[0] = (byte) columnNo;
			for (int i = 0; i < metadata.size(); i++) {
				ColumnMetadata col = metadata.get(i);
				cellLength += col.length;
				recHeader[i + 1] = col.code;
			}

			int rowId = fetchRowIdFromTableData(tableName);

			long fileLength = tableFile.length();
			int pages = (int) (fileLength / PAGE_SIZE);
			int pageNo = pages - 1;
			int pageStart = pageNo * PAGE_SIZE;

			tableFile.seek(pageStart + CONTENT_START);
			int contentStart = tableFile.readShort();

			tableFile.seek(pageStart + CELL_START_POSITIONS);

			long currentEmptyHeaderPos = tableFile.getFilePointer();
			while (tableFile.readShort() != 0) {
				currentEmptyHeaderPos = tableFile.getFilePointer();
			}

			if (contentStart - currentEmptyHeaderPos  + pageStart < cellLength) {
				pageNo = splitLeafPage(tableFile, tableName, pageNo, rowId);

				contentStart = PAGE_SIZE;
				pageStart = pageNo * PAGE_SIZE;
				currentEmptyHeaderPos = pageStart + CELL_START_POSITIONS;
			}

			int newContentStart = contentStart - cellLength;
			tableFile.seek(pageStart + newContentStart);
			tableFile.writeShort(cellLength - 6);
			tableFile.writeInt(++rowId);

			// Record header
			tableFile.write(recHeader);

			// Record body
			for (int i = 0; i < metadata.size(); i++) {
				ColumnMetadata col = metadata.get(i);
				String dataty = col.datatype.toUpperCase();
				String value = col.value;

				if (col.code == NULL) {
					continue;
				}

				switch (dataty) {
				case "TINYINT":
				case "BYTE":
					tableFile.write(Byte.parseByte(value));
					break;
				case "SMALLINT":
				case "SHORT":
					tableFile.writeShort(Integer.parseInt(value));
					break;
				case "INT":
				case "INTEGER":
					tableFile.writeInt(Integer.parseInt(value));
					break;
				case "BIGINT":
				case "LONG":
					tableFile.writeLong(Long.parseLong(value));
					break;
				case "FLOAT":
					tableFile.writeFloat(Float.parseFloat(value));
					break;
				case "DOUBLE":
				case "REAL":
					tableFile.writeDouble(Double.parseDouble(value));
					break;
				case "YEAR":
					tableFile.write(convertYearForWrite(value));
					break;
				case "TIME":
					tableFile.writeInt(Integer.parseInt(value)); // TODO convert to millis
					break;
				case "DATETIME":
					tableFile.writeLong(convertDateTimeForWrite(value));
					break;
				case "DATE":
					tableFile.writeLong(convertDateForWrite(value));
					break;
				case "TEXT":
					tableFile.writeBytes(value);
					break;
				}
			}

			// Update page header
			updateHeaderAfterContentWrite(tableFile, pageNo, newContentStart, currentEmptyHeaderPos);
			tableFile.close();
			increaseRecordCountAndLastId(tableName);

			tableFile.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}

	}

	public static boolean isConstraintViolated(RandomAccessFile tableFile, Map<Integer, ColumnMetadata> metadata,
			int ordinalPos, String compValue) throws IOException {

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
					ColumnMetadata col = metadata.get(i + 1);
					tableFile.seek(valuePos);
					if (code > 12) {
						int length = code - 12;
						byte[] b = new byte[length];
						tableFile.read(b);
						String val = new String(b);
						if (ordinalPos == i + 1 && compValue != null && val.equalsIgnoreCase(compValue)) {
							return true;
						}
					} else if (code == NULL) {
						if (ordinalPos == i + 1 && compValue == null) {
							return true;
						}
					} else if (code == TINYINT) {
						int b = tableFile.readByte();
						if (ordinalPos == i + 1 && compValue != null && Integer.parseInt(compValue) == b) {
							return true;
						}
					} else if (code == SMALLINT) {
						int b = tableFile.readShort();
						if (ordinalPos == i + 1 && compValue != null && Integer.parseInt(compValue) == b) {
							return true;
						}
					} else if (code == INT) {
						int b = tableFile.readInt();
						if (ordinalPos == i + 1 && compValue != null && Integer.parseInt(compValue) == b) {
							return true;
						}
					} else if (code == BIGINT) {
						long b = tableFile.readLong();
						if (ordinalPos == i + 1 && compValue != null && Long.parseLong(compValue) == b) {
							return true;
						}
					} else if (code == FLOAT) {
						float b = tableFile.readFloat();
						if (ordinalPos == i + 1 && compValue != null && Float.parseFloat(compValue) == b) {
							return true;
						}
					} else if (code == DOUBLE) {
						double b = tableFile.readDouble();
						if (ordinalPos == i + 1 && compValue != null && Double.parseDouble(compValue) == b) {
							return true;
						}
					} else if (code == YEAR) {
						int b = tableFile.readByte();
						if (ordinalPos == i + 1 && compValue != null && convertYearForWrite(compValue) == b) {
							return true;
						}
					} else if (code == TIME) {
						int b = tableFile.readInt();
						if (ordinalPos == i + 1 && compValue != null && Integer.parseInt(compValue) == b) {
							return true;
						}
					} else if (code == DATETIME) {
						long b = tableFile.readLong();
						if (ordinalPos == i + 1 && compValue != null && convertDateTimeForWrite(compValue) == b) {
							return true;
						}
					} else if (code == DATE) {
						long b = tableFile.readLong();
						if (ordinalPos == i + 1 && compValue != null && convertDateForWrite(compValue) == b) {
							return true;
						}
					}

					if (ordinalPos == i + 1) {
						break;
					}

					valuePos = tableFile.getFilePointer();
					i++;
				}

				cellPointer += 2;
				tableFile.seek(cellPointer);
				contentStart = tableFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tableFile.seek(rightPagePos);
			page = tableFile.readInt();
		}
		return false;
	}
}

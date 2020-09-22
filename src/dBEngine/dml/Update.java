package dBEngine.dml;

import static dBEngine.sdl.CatalogColumns.readColumnMetadata;
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
import static dBEngine.util.DatatypeUtil.convertDateForWrite;
import static dBEngine.util.DatatypeUtil.convertDateTimeForRead;
import static dBEngine.util.DatatypeUtil.convertDateTimeForWrite;
import static dBEngine.util.DatatypeUtil.convertYearForRead;
import static dBEngine.util.DatatypeUtil.convertYearForWrite;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dBEngine.data.ColumnMetadata;
import dBEngine.data.Condition;

public class Update {

	public static void run(ArrayList<String> commandTokens) {
		String tableName = commandTokens.get(1);

		try {
			RandomAccessFile tableFile = new RandomAccessFile(DAVISBASE_USER_DIR + tableName + ".tbl", "rw");
			Map<Integer, ColumnMetadata> metadata = readColumnMetadata(tableName);
			int updatedRecords = 0;
			int whereIndex = 0;
			boolean hasConditions = false;
			int i = 0;
			while (i < commandTokens.size()) {
				if (commandTokens.get(i).equalsIgnoreCase("where")) {
					whereIndex = i;
					hasConditions = true;
				}
				i++;
			}
			String updateValue = commandTokens.get(5);
			int strEnd = commandTokens.size();
			if (hasConditions) {
				strEnd = whereIndex;
			}
			int j = 6;
			while (j < strEnd) {
				updateValue += " " + commandTokens.get(j);
				j++;
			}

			int ordPos = metadata.values().stream().filter(c -> c.name.equalsIgnoreCase(commandTokens.get(3)))
					.findFirst().get().ordinal_position;

			if (!hasConditions) {
				updatedRecords = updateAllRecords(tableFile, metadata, ordPos, updateValue);
			} else {
				String conditionsOp = new String();
				Map<Integer, Condition> condMap = new HashMap<>();
				i = whereIndex + 1;
				j = 1;
				Condition cond = new Condition();
				String colName = commandTokens.get(i);
				cond.ordPos = metadata.values().stream().filter(c -> c.name.equalsIgnoreCase(colName)).findFirst()
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
					cond1.ordPos = metadata.values().stream().filter(c -> c.name.equalsIgnoreCase(colName1)).findFirst()
							.get().ordinal_position;
					++i;
					cond1.operator = commandTokens.get(i);
					++i;
					cond1.value = commandTokens.get(i);
					condMap.put(++j, cond1);
					i++;
				}

				updatedRecords = updateRecord(tableFile, metadata, condMap, conditionsOp, ordPos, updateValue);

			}

			System.out.println("Updated " + updatedRecords + " records");

			tableFile.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}

	}

	public static int updateAllRecords(RandomAccessFile tableFile, Map<Integer, ColumnMetadata> metadata, int ordPos,
			String updateValue) throws IOException {
		int updatedRecords = 0;

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
					tableFile.seek(valuePos);
					if (code > 12) {
						if (ordPos == i + 1) {
							tableFile.writeBytes(updateValue);
						} else {
							int length = code - 12;
							byte[] b = new byte[length];
							tableFile.read(b);
						}
					} else if (code == TINYINT) {
						if (ordPos == i + 1) {
							tableFile.writeByte(Integer.parseInt(updateValue));
						} else {
							tableFile.readByte();
						}
					} else if (code == SMALLINT) {
						if (ordPos == i + 1) {
							tableFile.writeShort(Integer.parseInt(updateValue));
						} else {
							tableFile.readShort();
						}
					} else if (code == INT) {
						if (ordPos == i + 1) {
							tableFile.writeInt(Integer.parseInt(updateValue));
						} else {
							tableFile.readInt();
						}
					} else if (code == BIGINT) {
						if (ordPos == i + 1) {
							tableFile.writeLong(Long.parseLong(updateValue));
						} else {
							tableFile.readLong();
						}
					} else if (code == FLOAT) {
						if (ordPos == i + 1) {
							tableFile.writeFloat(Float.parseFloat(updateValue));
						} else {
							tableFile.readFloat();
						}
					} else if (code == DOUBLE) {
						if (ordPos == i + 1) {
							tableFile.writeDouble(Double.parseDouble(updateValue));
						} else {
							tableFile.readDouble();
						}
					} else if (code == YEAR) {
						if (ordPos == i + 1) {
							tableFile.writeByte(convertYearForWrite(updateValue));
						} else {
							tableFile.readByte();
						}
					} else if (code == TIME) {
						if (ordPos == i + 1) {
							tableFile.writeInt(Integer.parseInt(updateValue));
						} else {
							tableFile.readInt();
						}
					} else if (code == DATETIME) {
						if (ordPos == i + 1) {
							tableFile.writeLong(convertDateTimeForWrite(updateValue));
						} else {
							tableFile.readLong();
						}
					} else if (code == DATE) {
						if (ordPos == i + 1) {
							tableFile.writeLong(convertDateForWrite(updateValue));
						} else {
							tableFile.readLong();
						}
					}

					valuePos = tableFile.getFilePointer();
					i++;
				}

				updatedRecords++;

				cellPointer += 2;
				tableFile.seek(cellPointer);
				contentStart = tableFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tableFile.seek(rightPagePos);
			page = tableFile.readInt();
		}

		return updatedRecords;

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

	public static int updateRecord(RandomAccessFile tableFile, Map<Integer, ColumnMetadata> metadata,
			Map<Integer, Condition> condMap, String condOp, int ordPos, String updateValue) throws IOException {
		int updatedRecords = 0;

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
					valuePos = colPos + columns;
					i = 0;
					while (i < columns) {
						tableFile.seek(colPos + i);
						byte code = tableFile.readByte();
						tableFile.seek(valuePos);
						if (code > 12) {
							if (ordPos == i + 1) {
								tableFile.writeBytes(updateValue);
							} else {
								int length = code - 12;
								byte[] b = new byte[length];
								tableFile.read(b);
							}
						} else if (code == TINYINT) {
							if (ordPos == i + 1) {
								tableFile.writeByte(Integer.parseInt(updateValue));
							} else {
								tableFile.readByte();
							}
						} else if (code == SMALLINT) {
							if (ordPos == i + 1) {
								tableFile.writeShort(Integer.parseInt(updateValue));
							} else {
								tableFile.readShort();
							}
						} else if (code == INT) {
							if (ordPos == i + 1) {
								tableFile.writeInt(Integer.parseInt(updateValue));
							} else {
								tableFile.readInt();
							}
						} else if (code == BIGINT) {
							if (ordPos == i + 1) {
								tableFile.writeLong(Long.parseLong(updateValue));
							} else {
								tableFile.readLong();
							}
						} else if (code == FLOAT) {
							if (ordPos == i + 1) {
								tableFile.writeFloat(Float.parseFloat(updateValue));
							} else {
								tableFile.readFloat();
							}
						} else if (code == DOUBLE) {
							if (ordPos == i + 1) {
								tableFile.writeDouble(Double.parseDouble(updateValue));
							} else {
								tableFile.readDouble();
							}
						} else if (code == YEAR) {
							if (ordPos == i + 1) {
								tableFile.writeByte(convertYearForWrite(updateValue));
							} else {
								tableFile.readByte();
							}
						} else if (code == TIME) {
							if (ordPos == i + 1) {
								tableFile.writeInt(Integer.parseInt(updateValue));
							} else {
								tableFile.readInt();
							}
						} else if (code == DATETIME) {
							if (ordPos == i + 1) {
								tableFile.writeLong(convertDateTimeForWrite(updateValue));
							} else {
								tableFile.readLong();
							}
						} else if (code == DATE) {
							if (ordPos == i + 1) {
								tableFile.writeLong(convertDateForWrite(updateValue));
							} else {
								tableFile.readLong();
							}
						}

						valuePos = tableFile.getFilePointer();
						i++;
					}

					updatedRecords++;

				}

				cellPointer += 2;
				tableFile.seek(cellPointer);
				contentStart = tableFile.readShort();
			}

			int rightPagePos = pageStart + RIGHT_PAGE;
			tableFile.seek(rightPagePos);
			page = tableFile.readInt();
		}
		return updatedRecords;
	}
}

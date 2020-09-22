package dBEngine.util;

import static dBEngine.util.Constants.CELL_COUNT;
import static dBEngine.util.Constants.INTERIOR_PAGE;
import static dBEngine.util.Constants.LEAF_PAGE;
import static dBEngine.util.Constants.PAGE_SIZE;
import static dBEngine.util.Constants.PARENT_PAGE;
import static dBEngine.util.Constants.RIGHT_PAGE;

import java.io.IOException;
import java.io.RandomAccessFile;

public class TableFileUtil {

	public static void setNewTableFileHeader(RandomAccessFile tableFile) throws IOException {
		tableFile.setLength(PAGE_SIZE);
		tableFile.seek(0);
		tableFile.write(LEAF_PAGE); // Table leaf page
		tableFile.write(0x00); // unused byte

		tableFile.writeShort(0x00); // Number of cells
		tableFile.writeShort(PAGE_SIZE); // Page start
		tableFile.writeInt(0xFFFFFFFF); // Right sibling
		tableFile.writeInt(0xFFFFFFFF); // Parent
		tableFile.writeShort(0x00); // unused bytes
	}

	public static void setParentPageHeader(RandomAccessFile tableFile, int pageNo, int rightmostChild)
			throws IOException {
		tableFile.seek(pageNo * PAGE_SIZE);
		tableFile.write(INTERIOR_PAGE); // Table interior page
		tableFile.write(0x00); // unused byte

		tableFile.writeShort(0x00); // Number of cells
		tableFile.writeShort(PAGE_SIZE); // Content start
		tableFile.writeInt(rightmostChild); // Right child
		tableFile.writeInt(0xFFFFFFFF); // Parent
		tableFile.writeShort(0x00); // unused bytes
	}

	public static void setRightPageHeader(RandomAccessFile tableFile, int pageNo, int parentPage, int pageType)
			throws IOException {
		tableFile.seek(pageNo * PAGE_SIZE);
		tableFile.write(pageType); // Leaf or interior page
		tableFile.write(0x00); // unused byte

		tableFile.writeShort(0x00); // Number of cells
		tableFile.writeShort(PAGE_SIZE); // Content start
		tableFile.writeInt(0xFFFFFFFF); // Right child
		tableFile.writeInt(parentPage); // Parent
		tableFile.writeShort(0x00); // unused bytes
	}

	public static void updateHeaderWithParent(RandomAccessFile tableFile, int pageNo, int parentPage)
			throws IOException {
		tableFile.seek(pageNo * PAGE_SIZE + PARENT_PAGE);
		tableFile.writeInt(parentPage); // Parent
	}

	public static void updateHeaderWithRightNode(RandomAccessFile tableFile, int pageNo, int rightPage)
			throws IOException {
		tableFile.seek(pageNo * PAGE_SIZE + RIGHT_PAGE);
		tableFile.writeInt(rightPage); // Rightmost child/sibling
	}

	public static void updateHeaderAfterContentWrite(RandomAccessFile tableFile, int pageNo, int newContentStart,
			long currentEmptyHeaderPos) throws IOException {
		int pageStart = pageNo * PAGE_SIZE;
		tableFile.seek(pageStart + CELL_COUNT);
		short cellCount = tableFile.readShort();
		tableFile.seek(pageStart + CELL_COUNT);
		tableFile.writeShort(++cellCount);
		tableFile.writeShort(newContentStart);
		tableFile.seek(currentEmptyHeaderPos);
		tableFile.writeShort(newContentStart);
	}
}

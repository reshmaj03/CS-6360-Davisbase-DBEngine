package dBEngine.data;

import static dBEngine.sdl.CatalogTables.updateRootPageInMetadata;
import static dBEngine.util.Constants.CELL_START_POSITIONS;
import static dBEngine.util.Constants.CONTENT_START;
import static dBEngine.util.Constants.INTERIOR_CELL_LENGTH;
import static dBEngine.util.Constants.INTERIOR_PAGE;
import static dBEngine.util.Constants.LEAF_PAGE;
import static dBEngine.util.Constants.PAGE_SIZE;
import static dBEngine.util.Constants.PARENT_PAGE;
import static dBEngine.util.TableFileUtil.setParentPageHeader;
import static dBEngine.util.TableFileUtil.setRightPageHeader;
import static dBEngine.util.TableFileUtil.updateHeaderAfterContentWrite;
import static dBEngine.util.TableFileUtil.updateHeaderWithParent;
import static dBEngine.util.TableFileUtil.updateHeaderWithRightNode;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BPlusTree {

	public static int splitPage(RandomAccessFile tableFile, String tableName, int pageNo, int rowId, int pageType)
			throws IOException {
		long fileLength = tableFile.length();
		int pages = (int) (fileLength / PAGE_SIZE);

		long pageStart = pageNo * PAGE_SIZE;
		tableFile.seek(pageStart + PARENT_PAGE); // Parent page
		int parentPage = tableFile.readInt();
		int rightMostPage = 0;
		if (parentPage == 0xFFFFFFFF) {
			parentPage = pages;
			rightMostPage = pages + 1;
			tableFile.setLength(PAGE_SIZE * (pages + 2));
			setParentPageHeader(tableFile, parentPage, rightMostPage);
			if (pageType == LEAF_PAGE) {
				setRightPageHeader(tableFile, rightMostPage, parentPage, LEAF_PAGE);
			} else {
				setRightPageHeader(tableFile, rightMostPage, parentPage, INTERIOR_PAGE);
			}

			updateHeaderWithParent(tableFile, pageNo, parentPage);
			updateRootPageInMetadata(tableName, parentPage);
			updateParentPage(tableFile, tableName, parentPage, pageNo, rowId);
		} else {

			parentPage = updateParentPage(tableFile, tableName, parentPage, pageNo, rowId);
			fileLength = tableFile.length();
			pages = (int) (fileLength / PAGE_SIZE);
			tableFile.setLength(PAGE_SIZE * (pages + 1));
			rightMostPage = pages; // Check
			if (pageType == LEAF_PAGE) {
				setRightPageHeader(tableFile, rightMostPage, parentPage, LEAF_PAGE);
			} else {
				setRightPageHeader(tableFile, rightMostPage, parentPage, INTERIOR_PAGE);
			}
			updateHeaderWithRightNode(tableFile, parentPage, rightMostPage);
		}

		if (pageType == LEAF_PAGE) {
			updateHeaderWithRightNode(tableFile, pageNo, rightMostPage); // Update right sibling for leaf page
		}
		return rightMostPage;

	}

	public static int splitLeafPage(RandomAccessFile tableFile, String tableName, int pageNo, int rowId)
			throws IOException {
		return splitPage(tableFile, tableName, pageNo, rowId, LEAF_PAGE);

	}

	public static int updateParentPage(RandomAccessFile tableFile, String tableName, int parentPageNo, int pageNo,
			int rowId) throws IOException {

		int cellLength = INTERIOR_CELL_LENGTH;
		int pageStart = parentPageNo * PAGE_SIZE;
		tableFile.seek(pageStart + CONTENT_START);
		int contentStart = tableFile.readShort();

		tableFile.seek(pageStart + CELL_START_POSITIONS);

		long currentEmptyHeaderPos = tableFile.getFilePointer();
		while (tableFile.readShort() != 0) {
			currentEmptyHeaderPos = tableFile.getFilePointer();
		}

		if (contentStart - currentEmptyHeaderPos + pageStart < cellLength) {
			parentPageNo = splitPage(tableFile, tableName, parentPageNo, rowId, INTERIOR_PAGE);
			contentStart = PAGE_SIZE;
			pageStart = parentPageNo * PAGE_SIZE;
			currentEmptyHeaderPos = pageStart + CELL_START_POSITIONS;
		}

		int newContentStart = contentStart - cellLength;
		tableFile.seek(pageStart + newContentStart);
		tableFile.writeInt(pageNo);
		tableFile.writeInt(rowId);

		updateHeaderAfterContentWrite(tableFile, parentPageNo, newContentStart, currentEmptyHeaderPos);

		return parentPageNo;
	}

}

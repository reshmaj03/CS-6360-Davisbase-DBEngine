package dBEngine.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatatypeUtil {

	public static int convertYearForWrite(String value) {
		int year = Integer.parseInt(value);
		return 2000 - year;
	}

	public static String convertYearForRead(int year) {
		return String.valueOf(2000 - year);
	}

	public static long convertDateForWrite(String dateString) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		try {
			Date date = (Date) format.parse(dateString);
			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Date().getTime();
	}

	public static String convertDateForRead(long date) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date d = new Date(date);
		return format.format(d);
	}

	public static long convertDateTimeForWrite(String dateTime) {
		String pattern = "yyyy-MM-dd_HH:mm:ss";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		try {
			Date date = format.parse(dateTime);
			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Date().getTime();
	}

	public static String convertDateTimeForRead(long date) {
		String pattern = "yyyy-MM-dd_HH:mm:ss";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date d = new Date(date);
		return format.format(d);
	}

}

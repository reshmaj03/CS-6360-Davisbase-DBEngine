package dBEngine.util;

public class PrintUtil {

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}
	
	public static String fixFormat(int len, String s) {
		return String.format("%-"+(len+3)+"s", s);
	}
}

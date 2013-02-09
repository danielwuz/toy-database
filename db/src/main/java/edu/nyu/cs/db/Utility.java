package edu.nyu.cs.db;

public class Utility {

	public static String trimAll(String str) {
		if (str == null) {
			return "";
		}
		return str.replaceAll("\\s", "");
	}
}

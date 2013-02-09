package edu.nyu.cs.db;

public class Log {

	/**
	 * Print out message
	 * 
	 * @param msg
	 *            message
	 */
	public static void print(Object msg) {
		System.out.println(msg);
	}

	public static void print_read(Site site, String variable, Object value) {
		System.out.println("Read " + variable + " from site " + site.getIndex()
				+ " value = " + value);
	}

	public static void warning(String string) {
		System.err.println(string);
	}

	public static void print_write(Site site, Object variable, Object value) {
		System.out.println("Write " + variable + " to site " + site.getIndex()
				+ " value = " + value);
	}

	public static void print_end(String transId, boolean commitable) {
		// TODO Auto-generated method stub

	}

	public static void abort(Transaction t) {
		System.out.println("Transaction " + t.getId()
				+ " aborted by wait-die-protocal");
	}

	public static void print_site(Site site) {
		if (!site.isRunning()) {
			System.out.println("Site " + site.getIndex() + " fails");
			return;
		}
		System.out.println(site);
	}

	public static void print_abort(String t) {
		System.out.println("Transaction " + t + " aborted.");
	}

	public static void print_commit(Transaction t) {
		System.out.println("Transaction " + t.getId() + " commited.");
	}

	public static void print_wait(Transaction t) {
		System.out.println("Transaction " + t.getId() + " wait because older");
	}
}

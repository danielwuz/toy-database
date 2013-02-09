/**
 * 
 */
package edu.nyu.cs.db;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Program entry.
 * <p>
 * 
 * 
 * @author Daniel Wu
 * 
 */
public class MainClass {

	private TM tm;

	private FileManager fm;

	private DM dm;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO check against input options

		String filePath = "";
		MainClass mc = null;
		try {
			mc = new MainClass(filePath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		}
		// program starts
		try {
			mc.run();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	public MainClass(String filePath) throws FileNotFoundException {
		// instantiate file manager
		fm = FileManager.createFileManager(filePath);
		// instantiate transaction manager
		tm = TM.instance();
		// initialize at time 0
		dm = DM.instance();
	}

	private void run() throws IOException {
		// read in command one at a time
		while (fm.hasNext()) {
			// next tick
			Clock.tiktok();
			Command[] commands = fm.nextCommands();
			tm.process(commands);
		}
	}

	public static class Clock {

		private static int time = 0;

		public static void tiktok() {
			time++;
		}

		public static int showTime() {
			return time;
		}
	}

}

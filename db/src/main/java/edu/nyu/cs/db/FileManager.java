package edu.nyu.cs.db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * File manager controls system input and output.
 * 
 * @author Daniel Wu
 * 
 */
public class FileManager {

	public static final String DELIMITER = ";";

	private BufferedReader input;

	private String nextLine = null;

	public FileManager(String filePath) throws FileNotFoundException {
		if (filePath != null && !filePath.trim().isEmpty()) {
			input = new BufferedReader(new FileReader(filePath));
		} else {
			input = new BufferedReader(new InputStreamReader(System.in));
		}
	}

	public static FileManager createFileManager(String filePath)
			throws FileNotFoundException {
		return new FileManager(filePath);
	}

	public boolean hasNext() throws IOException {
		nextLine = input.readLine();
		return nextLine != null;
	}

	public Command[] nextCommands() {
		assert nextLine != null;
		List<Command> result = new ArrayList<Command>();
		// parse command from current line
		String[] events = nextLine.split(DELIMITER);
		for (String event : events) {
			Command command = CommandFactory.parse(event);
			if (command != null) {
				result.add(command);
			}
		}
		return result.toArray(new Command[0]);
	}
}

package edu.nyu.cs.db;

/**
 * This interface defines commands that can be accepted and executed by the
 * system.
 * <p>
 * 
 * @author Daniel Wu
 * 
 */
public abstract class Command {

	protected String arg1;
	protected String arg2;
	protected String arg3;

	public Command(String param) {
		if (param == null || param.trim().isEmpty()) {
			return;
		}
		// parse value from given parameter, value seperated by comma
		String[] args = param.split(",");
		switch (args.length) {
		case 3:
			arg3 = args[2].trim();
		case 2:
			arg2 = args[1].trim();
		case 1:
			arg1 = args[0].trim();
		}
	}

	public abstract boolean execute() throws RuntimeException;

	public boolean isTransRequired() {
		return false;
	}

	@Override
	public String toString() {
		return "(" + arg1 + ", " + arg2 + ", " + arg3 + ")";
	}

	public static abstract class TransactionCommand extends Command {

		public TransactionCommand(String param) {
			super(param);
		}

		@Override
		public boolean execute() throws RuntimeException {
			return false;
		}

		@Override
		public boolean isTransRequired() {
			return true;
		}
		
	}

}

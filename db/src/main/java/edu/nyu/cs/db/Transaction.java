package edu.nyu.cs.db;

import java.util.HashSet;
import java.util.Set;

import edu.nyu.cs.db.MainClass.Clock;

/**
 * Transaction class.
 * <p>
 * Each transaction executes at most one operation in a time tick.
 * 
 * @author Daniel Wu
 * 
 */
public class Transaction {

	public static enum STATUS {
		NEW, COMPLETED, FAILED
	}

	public STATUS state;

	// transaction id
	private String id;

	public final int beginTime;

	// current operation
	private Command command = null;

	// read only transaction
	private boolean isReadOnly = false;

	// variables whose lock is held by current transaction
	private Set<String> variableIds = new HashSet<String>();

	/**
	 * Constructor with transaction id as parameter
	 * 
	 * @param id
	 *            transaction id
	 */
	public Transaction(String id) {
		this.id = id;
		this.beginTime = Clock.showTime();
	}

	/**
	 * Constructor with transaction id as readonly flag
	 * 
	 * @param id
	 *            transaction id
	 * @param isReadOnly
	 *            current transaction is readonly if <code>isReadOnly</code> is
	 *            true; otherwise it's read-write
	 */
	public Transaction(String id, boolean isReadOnly) {
		this.id = id;
		this.isReadOnly = isReadOnly;
		this.beginTime = Clock.showTime();
	}

	public void begin() {
		this.state = STATUS.NEW;
	}

	public void end() {
		this.state = STATUS.COMPLETED;
	}

	public String getId() {
		return id;
	}

	public Command getCommand() {
		return command;
	}

	/**
	 * @return true if current transaction is read only;return false if it is
	 *         read-write.
	 */
	public boolean isReadOnly() {
		return isReadOnly;
	}

	/**
	 * Checks if current transaction holds a lock for variable
	 * 
	 * @param varId
	 *            variable id
	 * @return true if current transaction holds a lock
	 */
	public boolean containsVar(String varId) {
		assert varId != null;
		return variableIds.contains(varId);
	}

	public void addVaraible(String var) {
		this.variableIds.add(var);
	}

	public static class TransactionException extends Exception {

		private static final long serialVersionUID = -8776761221998994073L;

		private Transaction t;

		private String message;

		public TransactionException(Transaction currentT) {
			this.t = currentT;
		}

		public TransactionException(Transaction currentT, String msg) {
			this.t = currentT;
			this.message = msg;
		}

		public TransactionException(String msg) {
			this.message = msg;
		}

		@Override
		public String getMessage() {
			return message;
		}

	}

	public boolean older(Transaction other) {
		return this.beginTime < other.beginTime;
	}

	@Override
	public String toString() {
		return "Transaction [id=" + id + ", beginTime=" + beginTime
				+ ", isReadOnly=" + isReadOnly + ", variableIds=" + variableIds
				+ "]";
	}

	public boolean commitable() {
		return state != STATUS.FAILED;
	}

}

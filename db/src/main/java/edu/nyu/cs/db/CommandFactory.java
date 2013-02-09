package edu.nyu.cs.db;

import java.lang.reflect.Constructor;
import java.util.Iterator;

import edu.nyu.cs.db.Command.TransactionCommand;

public class CommandFactory {

	public enum COMMANDS {
		BEGIN(Begin.class), BEGINRO(BeginRO.class), R(R.class), DUMP(Dump.class), W(
				W.class), END(End.class), ABORT(Abort.class), FAIL(Fail.class), RECOVER(
				Recover.class);

		private Class clazz;

		COMMANDS(Class clazz) {
			this.clazz = clazz;
		}

		/**
		 * Checks if input indicates this operation.
		 * 
		 * @param command
		 *            input command in string format.
		 * @return true if input issues this command; return false otherwise.
		 */
		public boolean test(String command) {
			// Checks input against regular expression
			String regex = this.name() + "(";
			return command.toUpperCase().startsWith(regex);
		}

		public Command createInstance(String command) {
			try {
				// parse parameters from command
				String param = parseParam(command);
				Constructor c = this.clazz.getConstructor(String.class);
				return (Command) c.newInstance(param);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

		private String parseParam(String command) {
			int start = command.indexOf('(');
			int end = command.indexOf(')');
			return command.substring(start + 1, end);
		}
	};

	public static Command parse(String command) {
		// remove all space
		command = Utility.trimAll(command);
		if (command.isEmpty()) {
			return null;
		}
		for (COMMANDS c : COMMANDS.values()) {
			if (c.test(command)) {
				return c.createInstance(command);
			}
		}
		Log.warning("Unrecognized command: " + command);
		return null;
	}

	/**
	 * Begin(T1) says that T1 begins.
	 * 
	 */
	public static class Begin extends Command {

		public Begin(String param) {
			super(param);
		}

		@Override
		public boolean execute() throws RuntimeException {
			String transactionId = super.arg1;
			// create read-write transaction
			Transaction t = new Transaction(transactionId);
			TM.instance().addTransaction(t);
			// transaction starts
			t.begin();
			return true;
		}

	}

	/**
	 * Begins a read-only transaction
	 * 
	 * @author Daniel Wu
	 * 
	 */
	public static class BeginRO extends Command {

		public BeginRO(String param) {
			super(param);
		}

		@Override
		public boolean execute() throws RuntimeException {
			String transactionId = super.arg1;
			// create a readonly transaction
			Transaction t = new Transaction(transactionId, true);
			TM.instance().addTransaction(t);
			// transaction starts
			t.begin();
			return true;
		}

		@Override
		public boolean isTransRequired() {
			return false;
		}

	}

	public static class R extends TransactionCommand {

		public R(String param) {
			super(param);
		}

		@Override
		public String toString() {
			return "R(" + arg1 + ", " + arg2 + ")";
		}

		public boolean execute(Site site, boolean isReadOnly) {
			String transId = this.arg1;
			String variable = this.arg2;
			// if current site doesn't contain variable
			if (!site.hasVariable(variable)) {
				return false;
			}
			// if variable is not ready for reading
			if (!site.isInitialized(variable)) {
				return false;
			}
			// if current transaction is read-only, obtain no locks
			if (isReadOnly) {
				int value = site.readInitial(transId, variable);
				Log.print_read(site, variable, value);
				return true;
			}
			// require a read lock
			if (site.lock(transId, variable, LockTable.READ)) {
				int value = site.read(variable);
				Log.print_read(site, variable, value);
				return true;
			}
			return false;
		}
	}

	public static class W extends TransactionCommand {

		public W(String param) {
			super(param);
		}

		public boolean execute(Site site) {
			String transId = this.arg1;
			String variable = this.arg2;
			int value = Integer.parseInt(this.arg3);
			// if current site doesn't contain variable, no need to write
			if (!site.hasVariable(variable)) {
				return true;
			}
			// require a write lock
			if (site.lock(transId, variable, LockTable.WRITE)) {
				site.write(variable, value);
				Log.print_write(site, variable, value);
				return true;
			}
			return false;
		}

		@Override
		public String toString() {
			return "W" + super.toString();
		}

	}

	public static class Dump extends Command {

		public Dump(String param) {
			super(param);
		}

		/**
		 * Shows the committed values of all copies of all variables at all
		 * sites, sorted per site
		 */
		public boolean execute() throws RuntimeException {
			if (arg1 == null) {
				dumpAll();
			} else if (arg1.startsWith("x")) {
				dumpVar(arg1);
			} else {
				dumpSite(arg1);
			}
			return true;
		}

		private void dumpSite(String siteIndex) {
			Iterator<Site> sites = DM.instance().iterator();
			while (sites.hasNext()) {
				Site site = sites.next();
				if (siteIndex.equals(site.getIndex())) {
					Log.print_site(site);
				}
			}
		}

		private void dumpVar(String var) {
			Iterator<Site> sites = DM.instance().iterator();
			while (sites.hasNext()) {
				Site site = sites.next();
				if (site.hasVariable(var)) {
					Log.print("Site " + site.getIndex() + ", " + var + "="
							+ site.read(var));
				}
			}
		}

		private void dumpAll() {
			Iterator<Site> sites = DM.instance().iterator();
			while (sites.hasNext()) {
				Site site = sites.next();
				Log.print_site(site);
			}
		}
	}

	public static class Abort extends End {

		public Abort(String param) {
			super(param);
		}

		@Override
		public boolean execute() throws RuntimeException {
			// transaction Id
			String transId = arg1;
			// abort and roll back
			abort(transId);
			// release all locks
			releaseLocks(transId);
			// destroy transaction by id
			destroyTransaction(transId);
			return true;
		}

	}

	public static class End extends TransactionCommand {

		public End(String param) {
			super(param);
		}

		@Override
		public boolean execute() throws RuntimeException {
			// transaction Id
			String transId = arg1;
			Transaction t = TM.instance().getTransaction(transId);
			// check if transaction can commit
			boolean commitable = t.commitable();
			// check if timeout
			boolean isTimeout = TM.instance().checkTimeout(transId);
			if (commitable && !isTimeout) {
				commit(t);
			} else {
				// abort and roll back
				abort(transId);
				Log.print("Site failed: " + !commitable + " Timeout: "
						+ isTimeout);
			}
			// release all locks
			releaseLocks(transId);
			// destroy transaction by id
			destroyTransaction(transId);
			Log.print_end(transId, commitable);
			return true;
		}

		protected void abort(String transId) {
			Transaction t = TM.instance().getTransaction(transId);
			Iterator<Site> sites = DM.instance().iterator();
			while (sites.hasNext()) {
				Site site = sites.next();
				if (!site.isRunning()) {
					continue;
				}
				site.abort(t);
			}
			Log.print_abort(transId);
		}

		private void commit(Transaction t) {
			Iterator<Site> sites = DM.instance().iterator();
			while (sites.hasNext()) {
				Site site = sites.next();
				if (!site.isRunning()) {
					continue;
				}
				site.commit(t);
			}
			Log.print_commit(t);
		}

		protected void destroyTransaction(String transId) {
			TM.instance().removeTransaction(transId);
		}

		protected void releaseLocks(String transId) {
			Iterator<Site> sites = DM.instance().iterator();
			while (sites.hasNext()) {
				Site site = sites.next();
				if (site.isRunning()) {
					// what about failed sites?
					site.releaseTransaction(transId);
				}
			}
		}

	}

	public static class Fail extends Command {

		public Fail(String param) {
			super(param);
		}

		@Override
		public boolean execute() throws RuntimeException {
			int siteId = Integer.parseInt(arg1);
			DM.instance().getSite(siteId).fail();
			return true;
		}

	}

	public static class Recover extends Command {

		public Recover(String param) {
			super(param);
		}

		@Override
		public boolean execute() throws RuntimeException {
			int siteId = Integer.parseInt(arg1);
			DM.instance().getSite(siteId).recovery();
			return true;
		}

	}

}

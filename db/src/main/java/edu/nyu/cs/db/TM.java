/**
 * 
 */
package edu.nyu.cs.db;

import java.awt.BufferCapabilities;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import edu.nyu.cs.db.Command.TransactionCommand;
import edu.nyu.cs.db.CommandFactory.R;
import edu.nyu.cs.db.CommandFactory.W;
import edu.nyu.cs.db.Transaction.TransactionException;

/**
 * Transaction Manager.
 * <p>
 * Transaction Manager translates read and write requests on variables to read
 * and write requests on copies using ``available copy algorithm''. Transaction
 * Manager never fails.
 * 
 * @author Daniel Wu
 * 
 */
public class TM {

	private Queue<Command> bufferedCommands;

	// <K,V>=<Transaction Id, transaction>
	private Map<String, Transaction> transactions;

	private static TM instance = null;

	/**
	 * Default constructor
	 */
	private TM() {
		this.transactions = new HashMap<String, Transaction>();
		this.bufferedCommands = new LinkedList<Command>();
	}

	/**
	 * Creates a singleton transaction manager.
	 * 
	 * @return transaction manager
	 */
	public static TM instance() {
		if (instance == null) {
			instance = new TM();
		}
		return instance;
	}

	/**
	 * Translates read and write requests on variables to read and write
	 * requests on copies using ``available copy algorithm''.
	 * 
	 * @param commands
	 *            co-temporous commands
	 */
	public void process(Command[] commands) {
		Queue<Command> commandQueue = appendToQueue(commands);
		processQueue(commandQueue);

	}

	private Queue<Command> appendToQueue(Command[] commands) {
		Queue<Command> queue = new LinkedList<Command>();
		// append historical commands
		while (!bufferedCommands.isEmpty()) {
			queue.offer(bufferedCommands.poll());
		}
		for (Command command : commands) {
			queue.offer(command);
		}
		return queue;
	}

	public void processQueue(Queue<Command> commands) {
		while (!commands.isEmpty()) {
			Command command = commands.poll();
			// if command does not require transaction
			// i.e. dump, fail, recover, queryState commands
			if (!command.isTransRequired()) {
				// execute directly
				command.execute();
				continue;
			}
			/*
			 * otherwise command requires a transaction,i.e.begin, end, read,
			 * write commands implement with available copy algorithm
			 */
			// check if transaction begins
			String transId = command.arg1;
			Transaction t = this.getTransaction(transId);
			// if transaction does not exist
			if (t == null) {
				Log.warning("Transaction " + transId + " does not exist");
				return;
			}
			// translate operation on variables to request on copies
			try {
				translate(command);
			} catch (TransactionException e) {
				// abort current transaction
				abort(transId);
				Log.abort(t);
			}
		}
	}

	private void abort(String transId) {
		String abortCommand = "Abort(" + transId + ")";
		CommandFactory.parse(abortCommand).execute();
	}

	/**
	 * Translates read/write on variables to request on copies
	 * 
	 * @param command
	 * @throws TransactionException
	 */
	private void translate(Command command) throws TransactionException {
		if (command instanceof R) {
			// if command is READ
			translateRead((R) command);
		} else if (command instanceof W) {
			// if command is WRITE
			translateWrite((W) command);
		} else {
			command.execute();
		}
	}

	private void translateWrite(W write) throws TransactionException {
		// iterate over available sites
		Iterator<Site> sites = DM.instance().iterator();
		if (isReadOnly(write)) {
			throw new TransactionException("Current transaction is read-only");
		}
		boolean success = true;
		// issues write all sites
		while (sites.hasNext()) {
			Site site = sites.next();
			if (!site.isRunning()) {
				continue;
			}
			if (!write.execute(site)) {
				// if write failed
				success = false;
			}
		}
		if (!success) {
			waitDieProtocal(write);
		}
	}

	private void translateRead(R read) throws TransactionException {
		// iterate over available sites
		Iterator<Site> sites = DM.instance().iterator();
		// check if current transaction is read-only
		boolean isReadOnly = isReadOnly(read);
		boolean successful = false;
		while (sites.hasNext()) {
			Site site = sites.next();
			if (!site.isRunning()) {
				continue;
			}
			if (read.execute(site, isReadOnly)) {
				// if site is running and read succeeds
				successful = true;
				break;
			}
		}
		if (!successful) {
			// wait-die protocal
			waitDieProtocal(read);
		}
	}

	private boolean isReadOnly(TransactionCommand read) {
		return this.getTransaction(read.arg1).isReadOnly();
	}

	private void waitDieProtocal(TransactionCommand command)
			throws TransactionException {
		// current transaction
		Transaction currentT = getTransaction(command.arg1);
		String varId = command.arg2;
		List<Transaction> otherTransactions = getTransactionsByVar(varId);
		for (Transaction other : otherTransactions) {
			// if any of the transactions that holds lock is older than current
			// transaction, then abort current one
			if (other.older(currentT)) {
				throw new TransactionException(currentT,
						"Transaction aborted by wait-die-protocal");
			}
		}
		// otherwise, put into waiting queue
		Log.print_wait(currentT);
		bufferedCommands.add(command);
	}

	/**
	 * Get all transactions which contains given variable id
	 * 
	 * @param varId
	 *            a variable id
	 * @return list of transactions
	 */
	private List<Transaction> getTransactionsByVar(String varId) {
		// all transactions
		Collection<Transaction> transObject = this.transactions.values();
		List<Transaction> result = new ArrayList<Transaction>();
		// iterate over all the transactions in current system
		for (Transaction transaction : transObject) {
			// if transaction contains variable
			if (transaction.containsVar(varId)) {
				result.add(transaction);
			}
		}
		return result;
	}

	/**
	 * Adds transaction to transaction manager
	 * 
	 * @param t
	 *            transaction
	 */
	public void addTransaction(Transaction t) {
		String id = t.getId();
		if (this.transactions.containsKey(id)) {
			System.out
					.println("TransactionManager already contains transaction "
							+ id + ", command ignored ");
		}
		// add in transaction
		this.transactions.put(id, t);
	}

	/**
	 * Retrieve transaction by id
	 * 
	 * @param id
	 *            transaction id
	 * @return transaction
	 * @throws RuntimeException
	 *             error if given transaction id doesn't exist
	 */
	public Transaction getTransaction(String id) {
		Transaction t = this.transactions.get(id);
		return t;
	}

	public void removeTransaction(String transId) {
		assert transId != null;
		removeCommands(transId);
		transactions.remove(transId);
	}

	private void removeCommands(String transId) {
		Queue<Command> newBuffer = new LinkedList<Command>();
		while (!bufferedCommands.isEmpty()) {
			Command c = bufferedCommands.poll();
			if (!transId.equals(c.arg1)) {
				newBuffer.add(c);
			}
		}
		bufferedCommands = newBuffer;
	}

	/**
	 * Transaction considered to be timeout if there still command in queue
	 * 
	 * @param transId
	 *            transaction id
	 * @return true if still command not execute at commit time
	 */
	public boolean checkTimeout(String transId) {
		Iterator<Command> it = bufferedCommands.iterator();
		while (it.hasNext()) {
			Command c = it.next();
			if (transId.equals(c.arg1)) {
				return true;
			}
		}
		return false;
	}
}

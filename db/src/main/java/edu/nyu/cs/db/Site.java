package edu.nyu.cs.db;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import edu.nyu.cs.db.Transaction.STATUS;

/**
 * Site class.
 * <p>
 * Each site represents a different machine.
 * 
 * @author Daniel Wu
 * 
 */
public class Site {

	// site index
	public final int index;

	private boolean running = true;

	private Map<String, Variable> copies = new TreeMap<String, Variable>(
			new Comparator<String>() {

				/**
				 * Compares by variable index numeric value
				 * 
				 * @param arg0
				 * @param arg1
				 * @return
				 */
				public int compare(String arg0, String arg1) {
					int v1 = Integer.parseInt(arg0.substring(1));
					int v2 = Integer.parseInt(arg1.substring(1));
					return v1 - v2;
				}
			});

	private LockTable locktable = new LockTable();

	// <K,V>=<variable name, value>
	private Map<String, Integer> staged = new HashMap<String, Integer>();

	private List<Transaction> listeners = new ArrayList<Transaction>();

	/**
	 * Constructor with site index as parameter
	 * 
	 * @param index
	 *            site index
	 */
	public Site(int index) {
		this.index = index;
		// initialize variables
		for (int i = 1; i <= DM.VAR_COUNT; i++) {
			String var = "x" + i;
			if (i % 2 == 0) {
				// even indexed variables are at all sites
				copies.put(var, new Variable(i));
			} else {
				// odd indexed variables are at one site each
				if ((1 + i) % DM.SITE_COUNT == index % DM.SITE_COUNT) {
					copies.put(var, new Variable(i));
				}
			}
		}
	}

	public boolean isRunning() {
		return running;
	}

	public void fail() {
		// notify all sites
		for (Transaction t : listeners) {
			t.state = STATUS.FAILED;
		}
		// clear up
		this.listeners.clear();
		this.staged.clear();
		this.locktable.clear();
		this.running = false;
	}

	public void recovery() {
		// all non-replicated variables are available for reads and writes
		// all replicated variables are available for writes but not for reads
		for (int i = 1; i <= DM.VAR_COUNT; i++) {
			String var = "x" + i;
			if (i % 2 == 0) {
				// even indexed variables are disabled until first write happens
				copies.get(var).inValid();
			}
		}

		this.running = true;
	}

	/**
	 * Checks if current variable available at current site.<br/>
	 * The odd indexed variables are at one site each. Even indexed variables
	 * are at all sites.
	 * 
	 * @param var
	 *            variable name
	 * @return true if current variable is available; return false otherwise.
	 */
	public boolean hasVariable(String var) {
		return copies.containsKey(var);
	}

	public boolean lock(String transId, String variable, String lock) {
		// two phase locking
		if (locktable.requireLock(transId, variable,lock)) {
			locktable.lock(transId, variable, lock);
			// register Transaction
			registerListener(transId);
			return true;
		}
		return false;
	}

	private void registerListener(String transId) {
		Transaction t = TM.instance().getTransaction(transId);
		listeners.add(t);
	}

	public int read(String variable) {
		if (staged.containsKey(variable)) {
			return staged.get(variable);
		}
		return copies.get(variable).value();
	}

	@Override
	public String toString() {
		return "Site " + index + ", copies=" + copies + locktable;
	}

	public String getIndex() {
		return "" + index;
	}

	public void write(String variable, int value) {
		// write to staged cache, but not commit yet
		staged.put(variable, value);
	}

	public void commit(Transaction transaction) {
		Set<String> stagedVariables = staged.keySet();
		List<String> removed = new ArrayList<String>();
		for (String varId : stagedVariables) {
			if (transaction.containsVar(varId)) {
				int value = staged.get(varId);
				copies.get(varId).setValue(value);
				// remove from staged status after committed
				removed.add(varId);
			}
		}
		// remove committed variables
		for (String varId : removed) {
			staged.remove(varId);
		}
	}

	public void releaseTransaction(String transId) {
		locktable.releaseTransaction(transId);
	}

	public boolean isInitialized(String var) {
		if (!hasVariable(var)) {
			return false;
		}
		if (staged.containsKey(var)) {
			return true;
		}
		Variable v = copies.get(var);
		return v.isValid();
	}

	public int readInitial(String transId, String var) {
		Variable v = copies.get(var);
		Transaction t = TM.instance().getTransaction(transId);
		// get value by transaction begin time
		int beginTime = t.beginTime;
		return v.valueByTime(beginTime);
	}

	public void abort(Transaction transaction) {
		Set<String> stagedVariables = staged.keySet();
		List<String> removed = new ArrayList<String>();
		for (String varId : stagedVariables) {
			if (transaction.containsVar(varId)) {
				// remove
				removed.add(varId);
			}
		}
		// remove from staged
		for (String varId : removed) {
			staged.remove(varId);
		}
	}

}

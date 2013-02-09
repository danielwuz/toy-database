package edu.nyu.cs.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Lock table class.
 * <p>
 * 
 * @author Daniel Wu
 * 
 */
public class LockTable {

	public static final String READ = "READ";

	public static final String WRITE = "WRITE";

	// shared read locks
	private Map<String, Set<String>> readLocks = new HashMap<String, Set<String>>();

	// exclusive write locks
	private Map<String, String> writeLocks = new HashMap<String, String>();

	public boolean requireLock(String transId, String variable, String lock) {
		// if no transaction contains current variable
		if (!lockExists(variable)) {
			return true;
		}
		if (READ.equals(lock)) {
			return requireReadLock(transId, variable);
		} else {
			return requireWriteLock(transId, variable);
		}

	}

	private boolean requireWriteLock(String transId, String variable) {
		if (writeLockHoldByOthers(transId, variable)) {
			return false;
		}
		Set<String> readLocks = readLockHolders(variable);
		if (readLocks == null || readLocks.isEmpty()) {
			return true;
		}
		if (!readLocks.contains(transId) || readLocks.size() > 1) {
			return false;
		}
		return true;
	}

	private boolean requireReadLock(String transId, String variable) {
		return !writeLockHoldByOthers(transId, variable);
	}

	private boolean writeLockHoldByOthers(String transId, String variable) {
		// check who holds write lock
		String anotherTransId = writeLockHolder(variable);
		// if write lock held by other transaction
		return anotherTransId != null && !anotherTransId.equals(transId);
	}

	private Set<String> readLockHolders(String variable) {
		return this.readLocks.get(variable);
	}

	private String writeLockHolder(String variable) {
		return writeLocks.get(variable);
	}

	private boolean lockExists(String variable) {
		return readLocks.containsKey(variable)
				|| writeLocks.containsKey(variable);
	}

	public void lock(String transId, String var, String lock) {
		if (lock.equals(LockTable.WRITE)) {
			// issue a write lock
			lockWrite(transId, var);
		} else {
			lockRead(transId, var);
		}
		TM.instance().getTransaction(transId).addVaraible(var);
	}

	private void lockRead(String transId, String var) {
		if (!readLocks.containsKey(var)) {
			Set<String> transactions = new HashSet<String>();
			readLocks.put(var, transactions);
		}
		// add in transaction id
		readLocks.get(var).add(transId);
	}

	private void lockWrite(String transId, String var) {
		this.writeLocks.put(var, transId);
	}

	@Override
	public String toString() {
		return " LockTable [readLocks=" + readLocks + ", writeLocks="
				+ writeLocks + "]";
	}

	public void releaseTransaction(String transId) {
		// release read lock
		releaseReadLock(transId);
		// release write lock
		releaseWriteLock(transId);
	}

	private void releaseWriteLock(String transId) {
		assert transId != null;
		Iterator<Entry<String, String>> it = writeLocks.entrySet().iterator();
		List<String> variable = new ArrayList<String>();
		while (it.hasNext()) {
			Entry<String, String> entry = it.next();
			// if transaction id matches
			if (transId.equals(entry.getValue())) {
				variable.add(entry.getKey());
			}
		}
		if (variable != null) {
			for (String var : variable) {
				writeLocks.remove(var);
			}
		}
	}

	private void releaseReadLock(String transId) {
		Iterator<Entry<String, Set<String>>> it = readLocks.entrySet()
				.iterator();
		while (it.hasNext()) {
			Entry<String, Set<String>> entry = it.next();
			Set<String> transactions = entry.getValue();
			// remove given transaction on current site
			transactions.remove(transId);
		}
	}

	public void clear() {
		this.readLocks.clear();
		this.writeLocks.clear();
	}
}

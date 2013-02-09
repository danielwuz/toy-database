package edu.nyu.cs.db;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.nyu.cs.db.MainClass.Clock;

/**
 * Variable class.
 * <p>
 * A variable shows the shared resources in current database.
 * 
 * @author Daniel Wu
 * 
 */
public class Variable {

	private int index;

	private int value;

	private boolean isValid;

	private final Map<Integer, Integer> valueStack = new HashMap<Integer, Integer>();

	/**
	 * Constructor with variable index as parameter.
	 * <p>
	 * Variable has initial value of (10 * index)
	 * 
	 * @param i
	 *            variable index
	 */
	public Variable(int i) {
		this.index = i;
		this.value = i * 10;
		this.isValid = true;
		valueStack.put(Clock.showTime(), this.value);
	}

	/**
	 * Returns current value
	 * 
	 * @return variable value
	 */
	public int value() {
		return this.value;
	}

	public boolean isValid() {
		return this.isValid;
	}

	public void inValid() {
		this.isValid = false;
	}

	@Override
	public String toString() {
		return isValid ? value + "" : "invalid";
	}

	public void setValue(int value) {
		// keep history
		valueStack.put(Clock.showTime(), value);
		this.value = value;
		// variable becomes valid after initialization
		this.isValid = true;
	}

	public int valueByTime(int beginTime) {
		Integer[] times = valueStack.keySet().toArray(new Integer[0]);
		Arrays.sort(times);
		// TODO
		for (int i = 0; i < times.length; i++) {
			if (times[i] > beginTime) {
				return valueStack.get(times[i - 1]);
			}
		}
		return this.value;
	}

}

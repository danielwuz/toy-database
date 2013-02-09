package edu.nyu.cs.db;

import java.util.Iterator;

/**
 * Data Manager
 * 
 * @author Daniel Wu
 * 
 */
public class DM implements Iterable<Site> {

	public static final int SITE_COUNT = 10;

	public static final int VAR_COUNT = 20;

	private Site[] sites;

	private static DM instance = new DM();

	public static DM instance() {
		return instance;
	}

	/**
	 * Default Constructor.
	 * <p>
	 * Initialize sites and variables
	 */
	private DM() {
		// initialize sites
		this.sites = new Site[SITE_COUNT + 1];
		for (int i = 1; i <= SITE_COUNT; i++) {
			sites[i] = new Site(i);
		}
	}

	@Override
	public Iterator<Site> iterator() {
		return new SiteIterator();
	}

	private class SiteIterator implements Iterator<Site> {

		private int index = 1;

		@Override
		public boolean hasNext() {
			return index < sites.length;
		}

		@Override
		public Site next() {
			return sites[index++];
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public Site getSite(int siteId) {
		// TODO
		return this.sites[siteId];
	}
}

/**
 * 
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Used to maintain a local timestamp in {@link HRegionServer} which is also
 * synced with the timestamps issued by the TSO
 * 
 * @author aran
 *
 */
public class TransactionTimestamp {
	/**
	 * 
	 */
	private AtomicLong latestTimestamp;

	public final static int LOCAL_BITS = 24;
	public final static long TRANSACTION_INC = 1L << LOCAL_BITS;
	private final static long LOCAL_MASK = TRANSACTION_INC - 1;
	private final static long TRANSACTION_MASK = ~LOCAL_MASK;

	/**
	 * Used to mark a singleton op to the HBase code, which would differentiate
	 * this from a regular HBase op (not related to Omid). Transactional ops are
	 * always marked with the TSO timestamp, which would be much smaller.
	 */
	public final static long SINGLETON_TIMESTAMP = HConstants.LATEST_TIMESTAMP - 1; 
	// Should probably choose something a bit more intelligent in the future :)

	
	public TransactionTimestamp() {
		latestTimestamp = new AtomicLong(0);
	}

	public long get() {
		return latestTimestamp.get();
	}

	private long increment() {
		// TODO: change the code so that increments will never overflow into the
		// TsoTimestamp part of the timestamp
		long ts = latestTimestamp.incrementAndGet();
		return ts;
	}

	/**
	 * 
	 * @param ts
	 *            The timestamp as issued by the TSO (i.e., shifted)
	 * @return the updated local timestamp
	 */
	private long update(long ts) {
		long currentTS;
		long updatedTS = TsoTimestampToRegionTimestamp(ts);
		do {
			currentTS = latestTimestamp.get();
		} while (currentTS < updatedTS
				&& !latestTimestamp.compareAndSet(currentTS, updatedTS));
		return Math.max(updatedTS, currentTS);
	}

	public static long TsoTimestampToRegionTimestamp(long ts) {
		return ts;
	}

	private long RegionTimestampToTsoTimestamp(long ts) {
		return ts & TRANSACTION_MASK;
	}

	private long getLocalTimestamp(long ts) {
		return ts & LOCAL_MASK;
	}

	/**
	 * 
	 * @return the most updated TSO timestamp this HRegionServer is aware of
	 */
	public long getTransactionTimestamp() {
		return RegionTimestampToTsoTimestamp(latestTimestamp.get());
	}

	/**
	 * 
	 * @return the local timestamp of the HRegionServer
	 */
	public long getLocalTimestamp() {
		return getLocalTimestamp(latestTimestamp.get());
	}

	public long updateByGet(Get get) {

		long ts = get.getTimeRange().getMax();
		if (ts == HConstants.LATEST_TIMESTAMP || ts == SINGLETON_TIMESTAMP)
			return ts;
		this.update(ts - 1); // the -1 is due to the get timerange max which is
								// non-inclusive (there was a + 1 in the get
								// itself)
		return TsoTimestampToRegionTimestamp(ts);
	}

	public long updateByMutatation(Mutation mt) {
		long ts = mt.getTimeStamp(); // This will return the read TSO timestamp
										// of the
										// transaction (if this is not a
										// singleton)

		if (ts == HConstants.LATEST_TIMESTAMP)
			return ts; // not a transaction mutation or a singleton - change
						// nothing
		if (ts == SINGLETON_TIMESTAMP) {
			// increase the local timestamp so it can be used as "now" to update
			// the KVs in the mutation
			return this.increment();
		} else { // This is a transactional mutation
			// Need to go over all the cells in the mutation and update
			// according to the latest
			// timestamp of each cell ?

			// this assumes that commit Puts set the Put timestamp to the
			// commitTimeStamp
			this.update(ts);
			return TsoTimestampToRegionTimestamp(ts);
		}
	}

	/**
	 * Checks whether a timestamp was created by a singleton write (based on the value of the lower bits of the timestamp)
	 * 
	 * @param ts timestamp to check
	 * @return true if the timestamp was created by a singleton write
	 */
	public static boolean isSingleton(long ts) {
		return ((ts & LOCAL_MASK) != 0);
	}

//	public static void updateMutationTS(Mutation mt, TransactionTimestamp ltt) {
//		long ts = mt.getTimeStamp();
//
//		if (ts == HConstants.LATEST_TIMESTAMP)
//			return;
//		// if this is a singleton write, use the latest local timestamp (ltt)
//		if (ts == SINGLETON_TIMESTAMP) {
//			// TODO: add an iteration over the mutation to update the cells to
//			// the local timestamp
//		}
//	}

	// public static Get updateGetTS(Get get) {
	// get.setTimeStamp(timestamp)
	// return get;
	// }
}

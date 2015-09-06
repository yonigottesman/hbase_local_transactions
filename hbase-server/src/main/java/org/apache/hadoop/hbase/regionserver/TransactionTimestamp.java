/**
 * 
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.HConstants;

/**
 * Used to maintain a local timestamp in {@link HRegionServer} which is also synced with the timestamps issued by the TSO
 * @author aran
 *
 */
public class TransactionTimestamp {
	/**
	 * 
	 */
	private AtomicLong latestTimestamp;
	
	private final static int 	LOCAL_BITS 			= 24;
	private final static long	TRANSACTION_INC		= 1 << LOCAL_BITS;
	private final static long 	LOCAL_MASK 			= TRANSACTION_INC - 1;
	private final static long 	TRANSACTION_MASK 	= ~LOCAL_MASK;
	
	/**
	 * Used to mark a singleton op to the HBase code, which would differentiate this from a regular HBase op (not related to Omid).
	 * Transactional ops are always marked with the TSO timestamp, which would be much smaller.
	 */
	public final static long 	SINGLETON_TIMESTAMP 	= HConstants.LATEST_TIMESTAMP - 1; // Should probably choose something a bit more intelligent in the future :)
	
	public TransactionTimestamp() {
		latestTimestamp = new AtomicLong(0);
	}
	
	public long increment() {
		// TODO: change the code so that increments will never overflow into the TRANSACTION part of the timestamp
		return latestTimestamp.incrementAndGet();
	}
	
	/**
	 * 
	 * @param ts The timestamp as issued by the TSO (i.e., not shifted)
	 * @return the updated local timestamp
	 */
	public long update(long ts) {
		long currentTS;
		long updatedTS = ts << LOCAL_BITS;
		do {
			currentTS = latestTimestamp.get();
		} while (currentTS < updatedTS && !latestTimestamp.compareAndSet(currentTS, updatedTS));
		return Math.max(updatedTS, currentTS);
	}
	
	/**
	 * 
	 * @return the most updated TSO timestamp this HRegionServer is aware of
	 */
	public long getTransactionTimestamp() {
		return latestTimestamp.get() & TRANSACTION_MASK >> LOCAL_BITS;
	}
	
	/**
	 * 
	 * @return the local timestamp of the HRegionServer
	 */
	public long getLocalTimestamp() {
		return latestTimestamp.get() & LOCAL_MASK;
	}
}

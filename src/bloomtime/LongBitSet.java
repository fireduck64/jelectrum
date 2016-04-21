
package bloomtime;

import java.util.BitSet;

/**
 * Interface for accessing a long bitset
 */
public interface LongBitSet
{
  void setBit(long index);
  boolean getBit(long index);

  BitSet getBitSetRange(long start, int len);

  /**
   * Ensure that all setBit operations are on disk
   */
  void flush();

 
  /**
   * Do any delayed write operations for full consistency and performance, implies flush
   */
  void cleanup();



}

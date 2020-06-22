package bloomtime;

import duckutil.TimeRecord;
import java.util.Set;
import java.util.TreeSet;
import java.util.BitSet;
import org.junit.Assert;

public class LongBitSetSimple implements LongBitSet
{
  private LongFile long_file;
  private TreeSet<Long> bits_to_set;

  private static final int MEM_MAX=1000000;

  public LongBitSetSimple(LongFile long_file)
  {
    this.long_file = long_file;
    bits_to_set = new TreeSet<Long>();

  }
  public synchronized void setBit(long index)
  {
    bits_to_set.add(index);
    if (bits_to_set.size() >= MEM_MAX)
    {
      flush();
    }
  }
  public synchronized boolean getBit(long index)
  {
    if (bits_to_set.contains(index)) return true;

    byte[] buff=new byte[1];
    long location = index/8;
    int bit_in_byte = (int) (index % 8);

    long_file.getBytes(location, buff);
    BitSet bs = BitSet.valueOf(buff);

    return bs.get(bit_in_byte);
  }

  public synchronized BitSet getBitSetRange(long start, int len)
  {
    Assert.assertEquals(0, start % 8);
    int byte_len = len / 8;
    if (len % 8 != 0) byte_len++;

    byte[] buff = new byte[byte_len];
    long location = start / 8;
    long_file.getBytes(location, buff);

    BitSet bs = BitSet.valueOf(buff);

    Set<Long> moar_bits = bits_to_set.subSet(start, start+len);
    for(long v : moar_bits)
    {
      int idx = (int)(v - start);
      bs.set(idx);
    }

    return bs;
  }

  /**
   * Ensure that all setBit operations are on disk
   */
  public synchronized void flush()
  {
    long t1 = System.nanoTime();

    byte[] buff = new byte[1];
    for(long index : bits_to_set)
    {
      long location = index/8;
      int bit_in_byte = (int) (index % 8);

      long_file.getBytes(location, buff);
      BitSet bs = BitSet.valueOf(buff);

      bs.set(bit_in_byte);
      byte[] save = bs.toByteArray();
      long_file.putBytes(location, save);
    }
    bits_to_set.clear();
    
    TimeRecord.record(t1, "LongBitSetSimple_flush");

  }

  
  public void cleanup()
  {
    flush();
  }



}

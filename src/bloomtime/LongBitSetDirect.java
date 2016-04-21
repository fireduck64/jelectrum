package bloomtime;

import jelectrum.TimeRecord;
import java.util.Set;
import java.util.TreeSet;
import java.util.BitSet;
import org.junit.Assert;

public class LongBitSetDirect implements LongBitSet
{
  private LongFile long_file;


  public LongBitSetDirect(LongFile long_file)
  {
    this.long_file = long_file;

  }
  public synchronized void setBit(long index)
  {
    long_file.setBit(index);
  }
  public synchronized boolean getBit(long index)
  {

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

    return bs;
  }

  public synchronized void flush()
  {

  }

  
  public void cleanup()
  {
    flush();
  }



}

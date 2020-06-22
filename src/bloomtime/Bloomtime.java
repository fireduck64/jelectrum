package bloomtime;

import java.nio.ByteBuffer;
import java.io.File;
import org.junit.Assert;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Set;
import java.util.BitSet;

import com.google.protobuf.ByteString;
import java.security.SecureRandom;
import java.util.Random;
import duckutil.TimeRecord;


/**
 * this number bear uses a thing called a "bloom filter" to tell if something is part of the group or not.
 * Sometimes it is wrong, and thinks something is in the group when it isn't.  But it is never wrong when it says something is not in.
 * This one uses many bloom filters next to each other so that you can check many groups at once.
 *
 * We call the groups slices here.
 */
public class Bloomtime
{
  private LongFile long_map;
  private int bloom_len;
  private int slices;
  private int hash_count;

  private TreeSet<Long> bits_to_set;

  public Bloomtime(File f, int slices, int bloom_len, int hash_count)
    throws java.io.IOException
  {
    this.slices = slices;
    this.bloom_len = bloom_len;
    this.hash_count = hash_count;

    bits_to_set = new TreeSet<Long>();

    Assert.assertTrue(slices > 0);
    Assert.assertTrue("slices must be divisible by 8", slices % 8 == 0);
    Assert.assertTrue(bloom_len > 0);
    Assert.assertTrue(hash_count > 0);
    
    long map_len = ((long)slices) * ((long)bloom_len) / 8L;

    try
    {
      long_map = new LongMappedBuffer(f, map_len);
    }
    catch(Throwable t)
    {
      System.out.println("Memory map failed, switching to file mode");
      System.gc();
      long_map = new LongRandomFile(f, map_len);
    }
  }

  /**
   * If doing a bunch of adds, this is a good idea.
   * The idea is that by sorting the bits to set and doing
   * them all at once, you take advantage of page locality
   * in the LongMappedBuffer.  If multiple bits are set in the same
   * page, they will be done next to each other so the page should
   * still be in memory.  The speed up from this seems to about 10x 
   * on fast SSD.  Probably more on worse things. Or nothing.  Maybe 
   * it does nothing.
   */
  public synchronized void accumulateBits(int slice, ByteString data)
  {
    Set<Integer> hashes = getHashIndexes(data);

    long t1 = System.nanoTime();
    for(int x : hashes)
    {
      long pos = (long)slices * (long)x + (long)slice;
      bits_to_set.add(pos);
    }
    TimeRecord.record(t1, "bloom_accumulatebits");
  }
  public synchronized void flushBits()
  {
    long t1 = System.nanoTime();
    for(long bit : bits_to_set)
    {
      long_map.setBit(bit);
    }
    bits_to_set.clear();
    TimeRecord.record(t1, "bloom_flush");
  }

  public synchronized void saveEntry(int slice, ByteString data)
  {
    Set<Integer> hashes = getHashIndexes(data);

    long t1 = System.nanoTime();
    for(int x : hashes)
    {
      long pos = (long)slices * (long)x + (long)slice;
      long_map.setBit(pos);
    }
    TimeRecord.record(t1, "bloom_setbit");
  }

  public Set<Integer> getMatchingSlices(ByteString data)
  {
    return getMatchingSlices(data, 0, slices);
  }
  public Set<Integer> getMatchingSlices(ByteString data, int start_slice, int end_slice)
  {
    long t1=System.nanoTime();
    while(start_slice % 8 != 0) start_slice--;
    while(end_slice % 8 != 0) end_slice++;
    end_slice = Math.min(end_slice, slices);

    Set<Integer> hashes = getHashIndexes(data);

    BitSet matches = null;
    Set<Integer> match_set = new HashSet<Integer>();

    int count = end_slice - start_slice;
    byte[] b = new byte[count / 8];
    for(int x : hashes)
    {
      long pos = ((long)slices * (long)x + start_slice) / 8L; 
      long t1_read = System.nanoTime();
      long_map.getBytes(pos, b);
      TimeRecord.record(t1_read, "bloom_read");

      long t1_bits = System.nanoTime();
      BitSet bs = BitSet.valueOf(b);
      if (matches == null)
      {
        matches = bs;
      }
      else
      {
        matches.and(bs);
      }
      TimeRecord.record(t1_bits, "bloom_bitkelp");
      if (matches.isEmpty())
      {
        TimeRecord.record(t1,"bloom_getmatch_short");
        return match_set;
      }
    }

    long t1_list=System.nanoTime();

    /*
     * Reading one bit at a time is slow (it was actually taking measurable clock time on a pi 2).
     * So splitting the bitset into longs and on a non-zero checking all those bits.  Quite a bit faster.
     */
    long[] vals = matches.toLongArray();
    for(int idx = 0; idx<vals.length; idx++)
    {
      if (vals[idx] != 0)
      {
        int end = Math.min((idx+1) * 64, slices);
        for(int i= idx * 64; i<end; i++)
        {
          if (matches.get(i)) match_set.add(i + start_slice);
        }

      }

    }
    /*for(int i=0; i<slices; i++)
    {
      if (matches.get(i)) match_set.add(i + start_slice);
    }*/
    TimeRecord.record(t1_list, "bloom_list");
    TimeRecord.record(t1_list, "bloom_slices", slices);

    TimeRecord.record(t1,"bloom_getmatch");
    return match_set;

  }

  public Set<Integer> getHashIndexes(ByteString data)
  {
    long t1 = System.nanoTime();
    Set<Integer> set = new HashSet<Integer>();

    //TODO - Only using 32 bits of entropy, which is crap
    //Make a better stream of nextInt()
    //SecureRandom sc = new SecureRandom(data.toByteArray());
    //Random sc = new Random(data.hashCode());
    Random sc = new DeterministicStream(data);

    for(int i=0; i<hash_count; i++)
    {
      int v = sc.nextInt(bloom_len);
      Assert.assertTrue(v >= 0);
      Assert.assertTrue(v < bloom_len);
      set.add(v);
    }
    TimeRecord.record(t1, "bloom_gethashindexes");

    return set;
  }



}

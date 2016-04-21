package bloomtime;

import jelectrum.DaemonThreadFactory;
import jelectrum.TimeRecord;
import java.util.Set;
import java.util.TreeSet;
import java.util.BitSet;
import java.util.HashMap;
import org.junit.Assert;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

public class LongBitSetThreaded implements LongBitSet
{
  private long file_bits;
  private LongFile long_file;
  private HashMap<Long, TreeSet<Long> > bits_to_set;
  private int in_memory=0;
  private long segment_len;

  private static final int THREADS=8;
  private static final long SEGMENTS=1024;
  private static final int MEM_MAX=1000000;

  private static ThreadPoolExecutor executor; 

  public LongBitSetThreaded(LongFile long_file, long file_bits)
  {
    this.long_file = long_file;
    this.file_bits = file_bits;
    segment_len = file_bits / SEGMENTS;
    bits_to_set = new HashMap<Long, TreeSet<Long> >(2048, 0.6f);
    initExec();
  }

  private static synchronized void initExec()
  {
    if (executor == null)
    {
      executor = new ThreadPoolExecutor(THREADS, THREADS, 2, TimeUnit.DAYS,
        new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory());
    }

  }
  public synchronized void setBit(long index)
  {
    long segment = index / segment_len;
    if (!bits_to_set.containsKey(segment))
    {
      bits_to_set.put(segment, new TreeSet<Long>());
    }
    bits_to_set.get(segment).add(index);
    in_memory++;
    if (in_memory >= MEM_MAX)
    {
      flush();
    }
  }

  public synchronized boolean getBit(long index)
  {
    long segment = index / segment_len;
    if (bits_to_set.containsKey(segment))
    {
      if (bits_to_set.get(segment).contains(index)) return true;
    }

    byte[] buff=new byte[1];
    long location = index/8;
    int bit_in_byte = (int) (index % 8);

    long_file.getBytes(location, buff);
    BitSet bs = BitSet.valueOf(buff);

    return bs.get(bit_in_byte);
  }

  public BitSet getBitSetRange(long start, int len)
  {
    Assert.assertEquals(0, start % 8);
    int byte_len = len / 8;
    if (len % 8 != 0) byte_len++;

    byte[] buff = new byte[byte_len];
    long location = start / 8;
    long_file.getBytes(location, buff);

    BitSet bs = BitSet.valueOf(buff);

    long segment = start / segment_len;
    long end = start + len;
    for(long seg = segment; seg * segment_len <= end; seg++)
    {
      if (bits_to_set.containsKey(seg))
      {
        Set<Long> moar_bits = bits_to_set.get(seg).subSet(start, start+len);
        for(long v : moar_bits)
        { 
          int idx = (int)(v - start);
          bs.set(idx);
        }
      }
    }
 

    return bs;
  }

  /**
   * Ensure that all setBit operations are on disk
   */
  public synchronized void flush()
  {

    long t1 = System.nanoTime();
    final Semaphore sem = new Semaphore(0);
    int count = 0;

    for(final Set<Long> bitset : bits_to_set.values())
    {
      count++;
      executor.execute(new Runnable(){
        public void run()
        {
          long t1_flush_seg = System.nanoTime();

          byte[] buff = new byte[1];
          for(long index : bitset)
          {
            long location = index/8;
            int bit_in_byte = (int) (index % 8);

            long_file.getBytes(location, buff);
            BitSet bs = BitSet.valueOf(buff);

            bs.set(bit_in_byte);
            byte[] save = bs.toByteArray();
            long_file.putBytes(location, save);
          }
          sem.release();
          TimeRecord.record(t1_flush_seg, "LongBitSetThreaded_flushseg");
        }
      });
    }
    TimeRecord.record(t1, "LongBitSetThreaded_flushstart");
    try{
    sem.acquire(count);
    }catch(InterruptedException e){e.printStackTrace();}
    bits_to_set.clear();
    in_memory = 0;

    TimeRecord.record(t1, "LongBitSetThreaded_flush");

  }

  
  public void cleanup()
  {
    flush();
  }



}

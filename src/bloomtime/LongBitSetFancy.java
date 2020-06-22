package bloomtime;

import jelectrum.DaemonThreadFactory;
import duckutil.TimeRecord;
import java.util.Set;
import java.util.TreeSet;
import java.util.BitSet;
import java.util.HashMap;
import org.junit.Assert;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

import java.io.File;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

public class LongBitSetFancy implements LongBitSet
{
  private long file_bits;
  private LongFile long_file;
  private HashMap<Long, SegmentHandler > segment_handlers;
  private int in_memory=0;
  private long segment_len;
  private File segment_dir;

  private static final int THREADS=32;
  private static final long SEGMENTS=256;
  private static final int MEM_MAX=1000000;

  private static ThreadPoolExecutor executor; 

  public LongBitSetFancy(File segment_d, LongFile long_file, long file_bits)
  {
    this.segment_dir = segment_d;
    this.long_file = long_file;
    this.file_bits = file_bits;
    segment_dir.mkdirs();
    segment_len = file_bits / SEGMENTS;
    segment_handlers = new HashMap<>((int)SEGMENTS*2, 0.6f);
    initExec();
    cleanup();
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
    try
    {
      long segment = index / segment_len;
      if (!segment_handlers.containsKey(segment))
      {
        segment_handlers.put(segment, new SegmentHandler(segment));
      }
      segment_handlers.get(segment).setBit(index);
      in_memory++;
      if (in_memory >= MEM_MAX)
      {
        cleanup();
      }
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);

    }
  }

  public synchronized boolean getBit(long index)
  {
    long segment = index / segment_len;
    if (segment_handlers.containsKey(segment))
    {
      if (segment_handlers.get(segment).getBit(index)) return true;
    }

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

    long segment = start / segment_len;
    long end = start + len;
    for(long seg = segment; seg * segment_len <= end; seg++)
    {
      if (segment_handlers.containsKey(seg))
      {
        Set<Long> moar_bits = segment_handlers.get(seg).bits_to_save.subSet(start, start+len);
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

    for(final SegmentHandler hand : segment_handlers.values())
    {
      count++;
      final OutputStream log = hand.append_log;
      executor.execute(new Runnable(){
        public void run()
        {
          long t1_seg_flush = System.nanoTime();
          try
          {
          log.flush();
          }
          catch(java.io.IOException e){e.printStackTrace(); System.exit(-1);}
          TimeRecord.record(t1_seg_flush, "LongBitSetFancy_flushseg");
          sem.release();
        }
      });
    }
    TimeRecord.record(t1, "LongBitSetFancy_flushstart");
    try{
    sem.acquire(count);
    }catch(InterruptedException e){e.printStackTrace();}

    TimeRecord.record(t1, "LongBitSetFancy_flush");

  }

  public void cleanup()
  {
    long t1=System.nanoTime();
    flush();
    segment_handlers.clear();
    System.gc();

    final Semaphore sem = new Semaphore(0);
    int count = 0;

    for(final File f : segment_dir.listFiles())
    {

      executor.execute(new Runnable(){
        public void run()
        {
          long t1_read = System.nanoTime();
          try
          {
            DataInputStream din = new DataInputStream(new FileInputStream(f));
            int read_count =0;
            try
            {
              while(true)
              {
                long v = din.readLong();
                long_file.setBit(v);
                read_count++;
              }
            }
            catch(java.io.EOFException e){}
            din.close();
            f.delete();


            sem.release();
          }
          catch(java.io.IOException e)
          {
            e.printStackTrace();
            System.exit(-1);
          }
          TimeRecord.record(t1_read, "LongBitSetFancy_segread");

        }

      });

      count ++;
      

    }

    try{
    sem.acquire(count);
    }catch(InterruptedException e){e.printStackTrace();}

    in_memory=0;

    TimeRecord.record(t1, "LongBitSetFancy_cleanup");
  }

  public class SegmentHandler
  {
    long segment;
    TreeSet<Long> bits_to_save;
    OutputStream append_log;
    File append_log_file;

    public SegmentHandler(long segment)
      throws java.io.IOException
    {
      this.segment = segment;
      bits_to_save = new TreeSet<>();
      append_log_file = new File(segment_dir, "seg_" + segment + ".log");
      append_log = new BufferedOutputStream(new FileOutputStream(append_log_file, true), 65536*8);

    }

    public synchronized void setBit(long v)
      throws java.io.IOException
    {
      bits_to_save.add(v);
      ByteBuffer bb = ByteBuffer.allocate(8);
      bb.putLong(v);
      append_log.write(bb.array());
    }
    public synchronized boolean getBit(long v)
    {
      return bits_to_save.contains(v);
    }

  }

}

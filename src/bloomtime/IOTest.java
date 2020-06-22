package bloomtime;

import java.io.File;
import java.util.Random;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.List;
import org.junit.Assert;
import java.text.DecimalFormat;
import jelectrum.StatData;
import duckutil.TimeRecord;
import java.util.concurrent.Semaphore;
import java.util.concurrent.LinkedBlockingQueue;

public class IOTest 
{
  public static void main(String args[]) throws Exception
  {
    new IOTest();
  }

  private LongFile long_file;
  private Semaphore sem;
  private LinkedBlockingQueue<List<Long> > queue;


  public IOTest() throws Exception 
  {
    TimeRecord tr=new TimeRecord();
    TimeRecord.setSharedRecord(tr);

    int rounds = 4;
    long bits = 25000000;
    long filesize = 20L * 1000L * 1000L * 1000L;

    queue = new LinkedBlockingQueue<>();

    File f = new File("/var/ssd/clash/iotest/20g");
    f.delete();

    long_file = new LongMappedBuffer(f, filesize);
    Random rnd = new Random();

    StatData round_time=new StatData();
    StatData rnd_time = new StatData();
    StatData write_time =new StatData();
    long t1_total = System.nanoTime();

    for(int i=0; i<16; i++)
    {
      new SaveThread().start();
    }
    TimeRecord.record(t1_total,"thread_start");


    for(int r = 0; r<rounds; r++)
    {
      long t1_make_list = System.nanoTime();
      //LinkedList<Long> bit_to_set = new LinkedList<>();
      HashMap<Long, LinkedList<Long> > bucket_map=new HashMap<>();
      for(long b = 0; b < bits; b++)
      {
        long z = BloomUtil.nextLong(rnd, filesize*8);
        long bucket = z / 1048576 ;
        if (!bucket_map.containsKey(bucket)) bucket_map.put(bucket, new LinkedList<Long>());

        bucket_map.get(bucket).add(z);
        //bit_to_set.add(z);
      }
      long t2_make_list = System.nanoTime();
      TimeRecord.record(t1_make_list,"make_list", bits);

      rnd_time.addDataPoint((t2_make_list-t1_make_list) / 1000000);
      
      sem = new Semaphore(0);

      long t1_set_bits = System.nanoTime();


      int count =0;
      for(List<Long> v : bucket_map.values())
      {
        queue.put(v);
        count++;
      }
      TimeRecord.record(t1_set_bits, "bucket_enqueue", count);
      sem.acquire(count);

      /*for(long b : bit_to_set)
      {
        long_file.setBit(b);
      }*/
      long t2_set_bits = System.nanoTime();

      round_time.addDataPoint((t2_set_bits - t1_make_list) / 1000000);
      write_time.addDataPoint((t2_set_bits - t1_set_bits) / 1000000);
      System.out.print('.');
    }
    System.out.println();

    DecimalFormat df = new DecimalFormat("0.000");
    rnd_time.print(  "Random time (ms)", df);
    write_time.print("Write time  (ms)", df);
    round_time.print("Round time  (ms)", df);

    long t2_total = System.nanoTime();
    double total_seconds = (t2_total - t1_total) / 1e9;
    double bits_sec = rounds * bits / total_seconds;
    long written = rounds * bits;
    System.out.println("Writen " + written + " in " + df.format(total_seconds) + " seconds at " + df.format(bits_sec) + " bits per second");
    tr.printReport(System.out);

  }

  public class SaveThread extends Thread
  {
    public SaveThread()
    {
      setDaemon(true);
      
    }

    public void run()
    {
      while(true)
      {
        try
        {
          List<Long> bit_to_set = queue.take();
          TreeSet<Long> in_order = new TreeSet<>();
      
          for(long b : bit_to_set)
          {
            long_file.setBit(b);
            //in_order.add(b);
          }
          for(long b : in_order)
          {
            long_file.setBit(b);
          }
          sem.release(1);
        }
        catch(Throwable t)
        {
          t.printStackTrace();
        }
      }
    }

  }

}

package jelectrum.db;

import jelectrum.Config;

import jelectrum.db.mongo.MongoDB;
import jelectrum.db.lobstack.LobstackDB;
import jelectrum.db.level.LevelDB;
import jelectrum.db.lmdb.LMDB;

import java.util.Map;
import java.util.TreeMap;
import com.google.protobuf.ByteString;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.LinkedBlockingQueue;
import java.text.DecimalFormat;
import com.google.bitcoin.core.Sha256Hash;
import jelectrum.EventLog;


public class GrindTest
{
  private static final long ITEMS_TO_ADD = 10000000L;
  private static final int ITEMS_PER_PUT = 100000;
  private static final int THREADS = 16;

  public static void main(String args[]) throws Exception
  {
    String name =args[0];
    new GrindTest(name);
    

  }

  DBMap db_map;
  Semaphore done_sem;
  LinkedBlockingQueue<String> queue;

  public GrindTest(String name)
    throws Exception
  {

    DB db = null;
    Config conf = new Config("jelly.conf");
    EventLog log =new EventLog(System.out);

    if (name.equals("mongo"))
    {
      db = new MongoDB(conf);
    }
    if (name.equals("lobstack"))
    {
      db = new LobstackDB(null, conf);
    }
    if (name.equals("leveldb"))
    {
      db = new LevelDB(log, conf);
    }
    if (name.equals("lmdb"))
    {
      db = new LMDB(conf);
    }

    db_map = db.openMap("grindtest");

    performTest();


  }

  private void performTest()
    throws Exception
  {
    done_sem = new Semaphore(0);
    queue = new LinkedBlockingQueue<>();
    int runs = (int)ITEMS_TO_ADD / ITEMS_PER_PUT;
    for(int i=0; i<runs; i++)
    {
      queue.put("" + i);
    }

    System.out.println("Adding " + ITEMS_TO_ADD + " in groups of " + ITEMS_PER_PUT);


    long total_start = System.nanoTime();

    for(int i=0; i<THREADS; i++) new GrindThread().start();

    done_sem.acquire(runs);
    long total_end = System.nanoTime();

    double sec = (total_end - total_start) / 1e9;

    DecimalFormat df = new DecimalFormat("0.000");

    System.out.println("Run done in " + df.format(sec));


  }


  public class GrindThread extends Thread
  {
    public GrindThread()
    {
      setName("GrindThread");
      setDaemon(true);
    }

    public void run()
    {
      Random rnd = new Random();
      while(true)
      {
        try
        {
          queue.take();

          TreeMap<String, ByteString> put_map = new TreeMap<String, ByteString>();

          for(int i=0; i< ITEMS_PER_PUT; i++)
          {
            put_map.put(randomHash(rnd).toString(), randomByteString(rnd));
          }
          db_map.putAll(put_map);
          done_sem.release();



        }
        catch(Throwable e)
        {
          e.printStackTrace();

        }



      }


    }

  }

  public static Sha256Hash randomHash(Random rnd)
  {
    byte[] b=new byte[32];
    rnd.nextBytes(b);
    return new Sha256Hash(b);
  }
  public static ByteString randomByteString(Random rnd)
  {
    byte[] b=new byte[100];
    rnd.nextBytes(b);

    return ByteString.copyFrom(b);
  }


}

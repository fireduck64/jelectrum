package jelectrum.db;

import jelectrum.TimeRecord;
import com.google.protobuf.ByteString;
import java.util.Map;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public abstract class DBMapThreaded extends DBMap
{
  protected Executor exec;

  public DBMapThreaded(Executor exec)
  {
    this.exec = exec;

  }

  @Override
  public void putAll(Map<String, ByteString> m)
  {
    final Semaphore sem = new Semaphore(0);
    int count = 0;
    for(Map.Entry<String, ByteString> me : m.entrySet())
    {
      final String key = me.getKey();
      final ByteString value = me.getValue();

      exec.execute(
        new Runnable()
        {
          public void run()
          {
            put(key, value);
            sem.release(1);
          }

        }
        );
      count++;
    }
    try
    {
      sem.acquire(count);
    }
    catch(InterruptedException e)
    {
      throw new RuntimeException(e);
    }

  }
 
}


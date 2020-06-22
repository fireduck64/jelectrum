package jelectrum.db;

import duckutil.TimeRecord;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;

import org.bitcoinj.core.Sha256Hash;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import com.google.protobuf.ByteString;


public abstract class DBMapMutationSetThreaded extends DBMapMutationSet
{
  protected Executor exec;

  public DBMapMutationSetThreaded(Executor exec)
  { 
    this.exec = exec;

  }

  @Override
  public void addAll(Collection<Map.Entry<ByteString, ByteString> > lst)
  {
    final Semaphore sem = new Semaphore(0);
    int count = 0;
    for(Map.Entry<ByteString, ByteString> me : lst)
    {
      final ByteString key = me.getKey();
      final ByteString val = me.getValue();

      exec.execute(
        new Runnable()
        {
          public void run()
          {
            add(key, val);
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

  @Override
  public void removeAll(Collection<Map.Entry<ByteString, ByteString>> lst)
  {
    final Semaphore sem = new Semaphore(0);
    int count = 0;
    for(Map.Entry<ByteString, ByteString> me : lst)
    {
      final ByteString key = me.getKey();
      final ByteString val = me.getValue();

      exec.execute(
        new Runnable()
        {
          public void run()
          {
            remove(key, val);
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

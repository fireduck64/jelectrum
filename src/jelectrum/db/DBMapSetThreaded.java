package jelectrum.db;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import duckutil.TimeRecord;

import org.bitcoinj.core.Sha256Hash;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;



public abstract class DBMapSetThreaded extends DBMapSet
{
  protected Executor exec;

  public DBMapSetThreaded(Executor exec)
  { 
    this.exec = exec;

  }

  @Override
  public void addAll(Collection<Map.Entry<String, Sha256Hash> > lst)
  {
    HashMap<String, List<Sha256Hash> > map_view = new HashMap<>();

    for(Map.Entry<String, Sha256Hash> me : lst)
    {
      String key = me.getKey();
      Sha256Hash h = me.getValue();
      if (!map_view.containsKey(key)) map_view.put(key, new LinkedList<Sha256Hash>());

      map_view.get(key).add(h);
    }

    final Semaphore sem = new Semaphore(0);
    int count = 0;
    for(Map.Entry<String, List<Sha256Hash> > me : map_view.entrySet())
    {
      final String key = me.getKey();
      final List<Sha256Hash> kl = me.getValue();

      exec.execute(
        new Runnable()
        {
          public void run()
          {
            for(Sha256Hash h : kl)
            {
              add(key, h);
            }
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

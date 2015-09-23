package jelectrum;

import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;


public class BandingMapSet<K,V> implements MapSet<K, V>
{
    private MapSet<K, V> inner;
    private long gatherWaitMs;
    private LinkedList<Map.Entry<K, V> > waiting_map;
    private SimpleFuture<V> future;
    private Object magic_lock;

    public BandingMapSet(MapSet<K,V> inner, long gatherWaitMs)
    {
      this.inner = inner;
      this.gatherWaitMs = gatherWaitMs;
      magic_lock = new Object();

      waiting_map = new LinkedList<Map.Entry<K, V> >();

      new BandThread().start();

    }

    public Set<V> getSet(Object key)
    {
      return inner.getSet(key);
    }

    public void add(K key, V value) 
    {
      SimpleFuture<V> f = null;
      synchronized(magic_lock)
      {
        if (future != null)
        {
          waiting_map.add(new java.util.AbstractMap.SimpleEntry<K,V>(key, value));
          f = future;
        }
        magic_lock.notifyAll();
      }
      if (f != null)
      {
        f.get();
        return;
      }

      inner.add(key,value);
    }
    public void addAll(Collection<Map.Entry<K, V> > lst)
    {
      inner.addAll(lst);
    }

    public class BandThread extends Thread
    {
      public BandThread()
      {
        setDaemon(true);
        setName("Bandthread");
      }
      public void run()
      {
        while(true)
        {
          doRun();

        }
      }

      private void doRun()
      {
        SimpleFuture<V> f = new SimpleFuture<V>();
        try
        {
          synchronized(magic_lock)
          {
            future = f;
            magic_lock.wait();
          }
          sleep(gatherWaitMs);
          synchronized(magic_lock)
          {
            future = null;
          }
          //System.out.println("Banding: " + waiting_map.size());
          inner.addAll(waiting_map);
          f.setResult(null);
          waiting_map.clear();
        }
        catch(Throwable t)
        {
          if (t instanceof RuntimeException)
          {
            f.setException((RuntimeException)t);
          }
          else
          {
            f.setException(new RuntimeException(t));
          }
        }

      }
    }

}

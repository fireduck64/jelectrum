package jelectrum;

import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.HashMap;


public class BandingMap<K,V> implements Map<K, V>
{
    private Map<K, V> inner;
    private long gatherWaitMs;
    private Map<K, V> waiting_map;
    private SimpleFuture<V> future;
    private Object magic_lock;

    public BandingMap(Map<K,V> inner, long gatherWaitMs)
    {
      this.inner = inner;
      this.gatherWaitMs = gatherWaitMs;
      magic_lock = new Object();

      waiting_map = new HashMap<K, V>(1024, 0.5f);

      new BandThread().start();

    }

    public void clear()
    {
      inner.clear();
    }


    public boolean containsKey(Object key)
    {
      return inner.containsKey(key);
    }
    public boolean containsValue(Object value)
    {
      return inner.containsValue(value);
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
      return inner.entrySet();
    }
    public  boolean equals(Object o)
    {
      return inner.equals(o);
    }

    public V get(Object key)
    {
      return inner.get(key);
    }
    public int hashCode() 
    {
      return inner.hashCode();
    }

    public boolean isEmpty()
    {
      return inner.isEmpty();
    }
    public int size()
    {
      return inner.size();
    }


    public  Set<K>  keySet()
    {
      return inner.keySet();
    }

    public V put(K key, V value) 
    {
      SimpleFuture<V> f = null;
      synchronized(magic_lock)
      {
        if (future != null)
        {
          waiting_map.put(key, value);
          f = future;
        }
        magic_lock.notifyAll();
      }
      if (f != null) return f.get();

      return inner.put(key,value);
    }
    public void putAll(Map<? extends K,? extends V> m) 
    {
      inner.putAll(m);
    }

    public V remove(Object key) 
    {
      return inner.remove(key);
    }

    public Collection<V> values() 
    {
      return inner.values();
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
          inner.putAll(waiting_map);
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

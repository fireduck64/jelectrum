package jelectrum;


import java.util.Map;
import java.util.Set;
import java.util.Collection;

public class CacheMap<K,V> implements Map<K,V>
{
    private LRUCache<K,V> cache;
    private Map<K,V> inner;

    public CacheMap(int cache_size, Map<K,V> inner)
    {
        this.inner = inner;
        cache = new LRUCache<K,V>(cache_size);


    }

    public void clear()
    {
        synchronized(cache)
        {
            cache.clear();
        }
        inner.clear();

    }


    public boolean containsKey(Object key)
    {
        synchronized(cache)
        {
            if (cache.containsKey(key)) return true;
        }

        return inner.containsKey(key);
    }
    public boolean containsValue(Object value)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public  boolean equals(Object o)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V get(Object key)
    {
        synchronized(cache)
        {
            V val = cache.get(key);
            if(val != null) return val;
        }
        V val = inner.get(key);
        if (val != null)
        {
            synchronized(cache)
            {
                cache.put((K)key, val);
            }
        }
        return val;

    }
    public int hashCode() 
    {
        throw new RuntimeException("not implemented - is stupid");
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
        throw new RuntimeException("not implemented - is stupid");
    }

    public V put(K key, V value) 
    {
        synchronized(cache)
        {
            cache.put(key, value);
        }
        return inner.put(key, value);
    }
    public void putAll(Map<? extends K,? extends V> m) 
    {
      synchronized(cache)
      {
        cache.putAll(m);
      }
      inner.putAll(m);

    }

    public V remove(Object key) 
    {
        synchronized(cache)
        {
            cache.remove(key);
        }

        return inner.remove(key);

    }

    public Collection<V>   values() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }


}

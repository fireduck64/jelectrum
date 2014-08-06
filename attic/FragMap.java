package jelectrum;

import org.mapdb.DB;

import java.util.Map;
import java.util.Set;
import java.util.Collection;

public class FragMap<K,V> implements Map<K,V>
{
    private int segments;
    private Map<K,V> frags[];

    public FragMap(DB db, String name, int segments)
    {
        this.segments = segments;

        frags = new Map[segments];



        for(int i=0; i<segments; i++)
        {
            frags[i] = db.getHashMap(name +"_" + i);
        }

    }

    public void clear()
    {
        for(int i=0; i<segments; i++)
        {
            frags[i].clear();
        }
    }


    public boolean containsKey(Object key)
    {
        int seg = getSegmentForObject(key);

        return frags[seg].containsKey(key);
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
        int seg = getSegmentForObject(key);

        return frags[seg].get(key);
    }
    public int hashCode() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public boolean isEmpty()
    {
        for(int i=0; i<segments; i++)
        {
            if (!frags[i].isEmpty()) return false;
        }
        return true;
    }
    public int size()
    {
        int sz = 0;
        for(int i=0; i<segments; i++)
        {
            sz += frags[i].size();
        }
        return sz;
    }


    public  Set<K>  keySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V put(K key, V value) 
    {
        int seg = getSegmentForObject(key);
        return frags[seg].put(key, value);
    }
    public void putAll(Map<? extends K,? extends V> m) 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V remove(Object key) 
    {

        int seg = getSegmentForObject(key);

        return frags[seg].remove(key);

    }

    public Collection<V>   values() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    private int getSegmentForObject(Object o)
    {
        int hc = o.hashCode();
        hc = Math.abs(hc);
        if (hc < 0) hc=0;
        int seg = hc % segments;

        return seg;


    }

}

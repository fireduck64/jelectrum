package jelectrum;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface MapSet<K,V>
{
  public void add(K key, V value);

  public void addAll(Collection<Map.Entry<K, V> > lst);

  public Set<V> getSet(Object key);
}

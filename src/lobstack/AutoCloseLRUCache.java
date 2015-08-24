
package lobstack;

import java.util.LinkedHashMap;
import java.util.Map;

public class AutoCloseLRUCache<K,V extends AutoCloseable> extends LinkedHashMap<K,V>
{

	private static final long serialVersionUID=9L;
	private int MAX_CAP;

	public AutoCloseLRUCache(int cap)
	{
		super(cap+1, 2.000f, true); 
		MAX_CAP=cap;

	}

	protected boolean removeEldestEntry(Map.Entry<K,V> eldest)
	{
    boolean close = (size() > MAX_CAP);

    if (close)
    {
      try
      {
        eldest.getValue().close();
      }
      catch(Exception e)
      {
        throw new RuntimeException(e);
      }
      return true;
    }

    return false;

	}

}

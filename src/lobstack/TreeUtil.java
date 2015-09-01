
package lobstack;

import java.util.Map;

public class TreeUtil
{
  public static void addTree(Map<Integer, Long> target, Map<Integer, Long> source)
  {
    for(Map.Entry<Integer, Long> me : source.entrySet())
    {
      addItem(target, me.getKey(), me.getValue());
    }

  }

  public static void addItem(Map<Integer, Long> target, int idx, long val)
  {
    if (target.containsKey(idx))
    {
      target.put(idx, target.get(idx) + val);
    }
    else
    {
      target.put(idx, val);
    }   
  }



}

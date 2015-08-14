package lobstack;

import java.util.TreeMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.SortedMap;

import java.nio.ByteBuffer;

import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class LobstackNode implements java.io.Serializable
{
  public static final int MAX_OPEN_FILES=512;

  private String prefix;
  private TreeMap<String, NodeEntry> children = new TreeMap<String, NodeEntry>();


  public LobstackNode(String prefix)
  {
    this.prefix = prefix;

  }

  public void printTree(Lobstack stack)
    throws IOException
  {
    System.out.print(prefix + " - " +  children.size());
    System.out.println();
    for(String key : children.keySet())
    {
      NodeEntry ne = children.get(key);
      if (ne.node)
      {
        stack.loadNodeAt(ne.location).printTree(stack);
      }
      else
      {
        System.out.println(key + " data " + ne.location);
      }
    }
  }

  public long putAll(Lobstack stack, TreeMap<Long, ByteBuffer> save_entries, Map<String, NodeEntry> put_map)
    throws IOException
  {

    // Add all as direct children

    children.putAll(put_map);

    // Make new subchildren as needed

    boolean keep_looking=true;
    while(keep_looking)
    {
      keep_looking=false;

      ArrayList<String> lst = new ArrayList<String>();
      lst.addAll(children.keySet());
      for(int i=0; i<lst.size()-1; i++)
      {
        String a = lst.get(i);
        String b = lst.get(i+1);
        NodeEntry ne_a = children.get(a);
        NodeEntry ne_b = children.get(b);

        // If b should go into a, and a is a node
        // just add it
        if ((b.startsWith(a)) && (ne_a.node))
        {
          TreeMap<String, NodeEntry> sub_put_map = new TreeMap<String, NodeEntry>();
          for(int j=i+1; j<lst.size(); j++)
          {
            String c = lst.get(j);
            if (c.startsWith(a))
            {
              sub_put_map.put(c, children.get(c));
            }
          }

          LobstackNode n = loadNode(stack, save_entries, ne_a.location);
          ne_a.location = n.putAll(stack, save_entries, sub_put_map);

          for(String s : sub_put_map.keySet())
          {
            children.remove(s);
          }



          keep_looking=true;
          break;
                   

        }

        int common = commonLength(a,b) - prefix.length();
        if (common > 0)
        {
          String common_prefix = a.substring(0, common + prefix.length());

          TreeMap<String, NodeEntry> sub_put_map = new TreeMap<String, NodeEntry>();
          for(int j=i+1; j<lst.size(); j++)
          {
            String c = lst.get(j);
            if (c.startsWith(common_prefix))
            {
              sub_put_map.put(c, children.get(c));
            }
          }

          LobstackNode n = new LobstackNode(common_prefix);
          NodeEntry ne = new NodeEntry();
          ne.node = true;
          ne.location = n.putAll(stack, save_entries, sub_put_map);

          for(String s : sub_put_map.keySet())
          {
            children.remove(s);
          }
          children.put(common_prefix, ne);

          keep_looking=true;
          break;
        }

      }
    }



    

    ByteBuffer self_buffer = serialize();
    long self_loc = stack.allocateSpace(self_buffer.capacity());
    save_entries.put(self_loc, self_buffer);
    return self_loc;
  }


  private LobstackNode loadNode(Lobstack stack, TreeMap<Long, ByteBuffer> save_entries, long location)
    throws IOException
  {
    if (save_entries.containsKey(location))
    {
      return LobstackNode.deserialize(save_entries.get(location));
    }
    else
    {
      return stack.loadNodeAt(location);
    }
  }

  /*private void removeSetFromMap(TreeMap<String, ByteBuffer> to_add, TreeSet<String> handled)
  {
    for(String key : handled)
    {
      to_add.remove(key);
    }
    handled.clear();
  }*/

  public ByteBuffer get(Lobstack stack, String key)
    throws IOException
  {
    if (key.equals(prefix))
    {
      return null;
    }
    if (children.containsKey(key))
    {
      NodeEntry ne = children.get(key);
      if (ne.node)
      {
        return stack.loadNodeAt(ne.location).get(stack, key);
      }
      else
      {
        return stack.loadAtLocation(ne.location);
      }
    }

    String sub = children.floorKey(key);
    if (sub != null)
    {
      if (key.startsWith(sub))
      {
        NodeEntry ne = children.get(sub);
        if (ne.node)
        {
          return stack.loadNodeAt(ne.location).get(stack,key);
        }
 
      }
    }
    return null;

  }

  public Map<String, ByteBuffer> getByPrefix(Lobstack stack, String key_prefix)
    throws IOException
  {
    

    TreeMap<String, ByteBuffer> ret = new TreeMap<String, ByteBuffer>();

    for(String sub : children.keySet())
    {
      if ((sub.startsWith(key_prefix)) || (key_prefix.startsWith(sub)))
      {
        NodeEntry ne = children.get(sub);
        if (ne.node)
        {
          ret.putAll(
            stack.loadNodeAt(ne.location).getByPrefix(stack, key_prefix)
             );

        }
        else
        {
          ret.put(sub, stack.loadAtLocation(ne.location));
        }
        

      }

    }

    return ret;



  }



  public ByteBuffer serialize()
  {
    try
    {
      ByteArrayOutputStream b_out = new ByteArrayOutputStream();
      GZIPOutputStream z_out = new GZIPOutputStream(b_out);
      ObjectOutputStream o_out = new ObjectOutputStream(z_out);
  
      o_out.writeObject(this);
      o_out.flush();
      o_out.close();

      return ByteBuffer.wrap(b_out.toByteArray());
    }
    catch(Exception e) { throw new RuntimeException(e);}
    
  }

  public static LobstackNode deserialize(ByteBuffer buf)
  {
    try
    {
      ByteArrayInputStream b_in = new ByteArrayInputStream(buf.array());
      GZIPInputStream z_in = new GZIPInputStream(b_in);
      ObjectInputStream o_in = new ObjectInputStream(z_in);

      return (LobstackNode) o_in.readObject();

    }
    catch(Exception e) { throw new RuntimeException(e);}
  }

  public static int commonLength(String a, String b)
  {
    int max = Math.min(a.length(), b.length());

    int same = 0;
    for(int i=0; i<max; i++)
    {
      if (a.charAt(i) == b.charAt(i)) same++;
      else break;
    }
    //if (same % 2 == 1) same--;
    return same;

  }


}

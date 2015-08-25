package lobstack;

import java.util.TreeMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.SortedMap;

import java.nio.ByteBuffer;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executor;

public class LobstackNode implements java.io.Serializable
{

  // If anything else is added, serialize and deserialize need to be updated
  private String prefix;
  private TreeMap<String, NodeEntry> children;

  public static int NODE_VERSION=-2;



  public LobstackNode(String prefix)
  {
    this.prefix = prefix;
    children = new TreeMap<String, NodeEntry>();

  }
  public LobstackNode(String prefix, TreeMap<String, NodeEntry> children)
  {
    this.prefix = prefix;
    this.children = children;
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
        System.out.println(key + " data @" + ne.location + " - bytes:" + stack.loadAtLocation(ne.location).capacity());
      }
    }
  }
  public void getTreeStats(Lobstack stack, TreeStat stats)
    throws IOException
  {
    synchronized(stats)
    {
      stats.node_children+=children.size();
      stats.node_children_min = Math.min(stats.node_children_min, children.size());
      stats.node_children_max = Math.max(stats.node_children_max, children.size());
    }

    for(String key : children.keySet())
    {
      NodeEntry ne = children.get(key);
      int sz =  stack.loadSizeAtLocation(ne.location);
      stats.addFileUse(ne.location, sz + 4);
      if (ne.node)
      {
        LobstackNode n = stack.loadNodeAt(ne.location);
        synchronized(stats)
        {
          stats.node_size += sz + 4;
          stats.node_count++;
        } 


        n.getTreeStats(stack, stats);

      }
      else
      {
        synchronized(stats)
        {
          stats.data_count++;
          stats.data_size+= sz + 4;
        }
      }
    }
  }

 
  public void getAll(Lobstack stack, BlockingQueue<Map.Entry<String, ByteBuffer> > consumer)
    throws IOException, InterruptedException
  {
    for(String key : children.keySet())
    {
      NodeEntry ne = children.get(key);
      if (ne.node)
      {
        stack.loadNodeAt(ne.location).getAll(stack, consumer);
      }
      else
      {
        String data_key = key.substring(0, key.length()-1);
        consumer.put(new SimpleEntry<String,ByteBuffer>(data_key, stack.loadAtLocation(ne.location)));
      }
    }
   
  }

  public long estimateReposition(Lobstack stack, int min_file)
    throws IOException
  {
    long sz = 0;

    for(Map.Entry<String, NodeEntry> me : children.entrySet())
    {
      String str = me.getKey();
      NodeEntry ne = me.getValue();
      if (ne.min_file_number < min_file)
      {
        sz += stack.loadSizeAtLocation(ne.location) + 4;
        if (ne.node)
        {
          LobstackNode n = stack.loadNodeAt(ne.location); 
          sz += n.estimateReposition(stack, min_file);
        }
      }
    }
    sz += serialize().capacity() + 4;
    return sz;

  }


  public NodeEntry reposition(Lobstack stack, TreeMap<Long, ByteBuffer> save_entries, int min_file)
    throws IOException
  {
    for(Map.Entry<String, NodeEntry> me : children.entrySet())
    {
      String str = me.getKey();
      NodeEntry ne = me.getValue();
      if (ne.min_file_number < min_file)
      {
        if (ne.node)
        {
          LobstackNode n = loadNode(stack, save_entries, ne.location); 
          ne = n.reposition(stack, save_entries, min_file);
          children.put(str, ne);
        }
        else
        {
          ByteBuffer data = stack.loadAtLocation(ne.location);
          ByteBuffer comp = stack.compress(data);

          ne.location = stack.allocateSpace(comp.capacity());
          ne.min_file_number = (int)(ne.location / Lobstack.SEGMENT_FILE_SIZE);
          save_entries.put(ne.location, comp);
          children.put(str, ne);
        }

      }

    }

    ByteBuffer self_buffer = serialize();
    ByteBuffer comp = stack.compress(self_buffer);
    long self_loc = stack.allocateSpace(comp.capacity());
    save_entries.put(self_loc, comp);

    NodeEntry my_entry = new NodeEntry();
    my_entry.node = true;
    my_entry.location = self_loc;
    my_entry.min_file_number = (int) (self_loc / Lobstack.SEGMENT_FILE_SIZE);
    for(NodeEntry ne : children.values())
    {
      my_entry.min_file_number = Math.min(my_entry.min_file_number, ne.min_file_number);
    }
    return my_entry;

  }

  public int getMinFileNumber(long location)
  {
    int min_file_number = (int) (location / Lobstack.SEGMENT_FILE_SIZE);
    for(NodeEntry ne : children.values())
    {
      min_file_number = Math.min(min_file_number, ne.min_file_number);
    }

    return min_file_number;
  }

  public NodeEntry putAll(Lobstack stack, TreeMap<Long, ByteBuffer> save_entries, Map<String, NodeEntry> put_map)
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
          ne_a = n.putAll(stack, save_entries, sub_put_map);
          children.put(a, ne_a);

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
          NodeEntry ne = n.putAll(stack, save_entries, sub_put_map);

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
    ByteBuffer comp = stack.compress(self_buffer);
    long self_loc = stack.allocateSpace(comp.capacity());
    save_entries.put(self_loc, comp);

    NodeEntry my_entry = new NodeEntry();
    my_entry.node = true;
    my_entry.location = self_loc;
    my_entry.min_file_number = (int) (self_loc / Lobstack.SEGMENT_FILE_SIZE);
    for(NodeEntry ne : children.values())
    {
      my_entry.min_file_number = Math.min(my_entry.min_file_number, ne.min_file_number);
    }
    return my_entry;
  }


  private LobstackNode loadNode(Lobstack stack, TreeMap<Long, ByteBuffer> save_entries, long location)
    throws IOException
  {
    if (save_entries.containsKey(location))
    {
      return LobstackNode.deserialize(stack.decompress(save_entries.get(location)));
    }
    else
    {
      return stack.loadNodeAt(location);
    }
  }

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
      DataOutputStream d_out = new DataOutputStream(b_out);

      SerialUtil.writeString(d_out, prefix);
      d_out.writeInt(NODE_VERSION);
      d_out.writeInt(children.size());
      for(Map.Entry<String, NodeEntry> me : children.entrySet())
      {
        String sub = me.getKey();
        NodeEntry ne = me.getValue();
        String subsub = sub.substring(prefix.length());
        SerialUtil.writeString(d_out, subsub);
        d_out.writeBoolean(ne.node);
        d_out.writeLong(ne.location);
        d_out.writeInt(ne.min_file_number);
      }
      

      d_out.flush();
      d_out.close();

      return ByteBuffer.wrap(b_out.toByteArray());
    }
    catch(Exception e) { throw new RuntimeException(e);}
    
  }


  public static LobstackNode deserialize(ByteBuffer buf)
  {
    try
    {
      ByteArrayInputStream b_in = new ByteArrayInputStream(buf.array());
      DataInputStream d_in = new DataInputStream(b_in);

      String prefix = SerialUtil.readString(d_in);
      int v = d_in.readInt();
      int read_version=-1;
      if (v < 0)
      {
        read_version=v;
      }
      int count;
      if (read_version==-1)
      {
        count = v;
      }
      else
      {
        count = d_in.readInt();
      }

      TreeMap<String, NodeEntry> c = new TreeMap<String, NodeEntry>();
      for(int i=0; i<count; i++)
      {
        String sub = prefix + SerialUtil.readString(d_in);
        NodeEntry ne = new NodeEntry();
        ne.node = d_in.readBoolean();
        ne.location = d_in.readLong();
        if (read_version <= -2)
        {
          ne.min_file_number = d_in.readInt();
        }
        c.put(sub, ne);
      }

      return new LobstackNode(prefix, c);


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
    if (same % 2 == 1) same--;
    return same;

  }


}

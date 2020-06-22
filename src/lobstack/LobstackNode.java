package lobstack;

import java.util.TreeMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.SortedMap;

import java.nio.ByteBuffer;

import org.junit.Assert;
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
import duckutil.TimeRecord;

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
    /*if (children.size() > 256)
    {
      System.out.println("" + children.size() + " - " + prefix + ": " + children.keySet());
    }*/

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

  /**
   * Consider all repositions of files less than or equal to max_file
   */
  public TreeMap<Integer, Long> estimateReposition(Lobstack stack, int max_file)
    throws IOException
  {
    TreeMap<Integer, Long> sz_map = new TreeMap<Integer, Long>();

    TreeMap<String, WorkUnit> work_map = new TreeMap<String, WorkUnit>();

    for(Map.Entry<String, NodeEntry> me : children.entrySet())
    {
      String str = me.getKey();
      NodeEntry ne = me.getValue();
      if (ne.min_file_number < max_file)
      {
        long sz = stack.loadSizeAtLocation(ne.location) + 4;
        for(int idx = (int) (ne.location / Lobstack.SEGMENT_FILE_SIZE) + 1; idx<=max_file; idx++)
        {
          TreeUtil.addItem(sz_map, idx, sz);
        }
        if (ne.node)
        {
          LobstackNode n = stack.loadNodeAt(ne.location);
          WorkUnit wu = new WorkUnit(stack,n, max_file);
          if (stack.getQueue().offer(wu))
          {
            work_map.put(str, wu);
          }
          else
          {
            TreeUtil.addTree(sz_map, n.estimateReposition(stack, max_file));
          }
        }
      }
    }

    for(Map.Entry<String, WorkUnit> me : work_map.entrySet())
    {
      TreeUtil.addTree(sz_map, me.getValue().estimate.get());
    }

    return sz_map;

  }


  public NodeEntry reposition(Lobstack stack, TreeMap<Long, ByteBuffer> save_entries, int min_file)
    throws IOException
  {
    TreeMap<String, WorkUnit> work_map = new TreeMap<String, WorkUnit>();

    for(Map.Entry<String, NodeEntry> me : children.entrySet())
    {
      String str = me.getKey();
      NodeEntry ne = me.getValue();
      if (ne.min_file_number < min_file)
      {
        if (ne.node)
        {
          LobstackNode n = loadNode(stack, save_entries, ne.location); 
          WorkUnit wu = new WorkUnit(stack, n, min_file, save_entries);
          if (stack.getQueue().offer(wu))
          {
            work_map.put(str, wu);
          }
          else
          {
            ne = n.reposition(stack, save_entries, min_file);
            children.put(str, ne);
          }
        }
        else
        {
          ByteBuffer data = stack.loadAtLocation(ne.location);
          ByteBuffer comp = stack.compress(data);

          ne.location = stack.allocateSpace(comp.capacity());
          ne.min_file_number = (int)(ne.location / Lobstack.SEGMENT_FILE_SIZE);
          synchronized(save_entries)
          {
            save_entries.put(ne.location, comp);
          }
          children.put(str, ne);
        }
      }

    }

    for(Map.Entry<String, WorkUnit> me : work_map.entrySet())
    {
      NodeEntry ne = me.getValue().return_entry.get();
      children.put(me.getKey(), ne);
    }

    ByteBuffer self_buffer = serialize();
    ByteBuffer comp = stack.compress(self_buffer);
    long self_loc = stack.allocateSpace(comp.capacity());
    synchronized(save_entries)
    {
      save_entries.put(self_loc, comp);
    }

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
    TimeRecord tr = stack.getTimeReport();

   
    // Add all as direct children

    children.putAll(put_map);

    TreeMap<String, WorkUnit> working_map = new TreeMap<String, WorkUnit>();
    for(Map.Entry<String, NodeEntry> me : children.entrySet())
    {
      working_map.put(me.getKey(), new WorkUnit(stack, me.getValue(), save_entries));
    }

    //Just to make sure they are all from new entries 
    children.clear();


    boolean keep_looking=true;
    while(keep_looking)
    {
      keep_looking=false;

      ArrayList<String> lst = new ArrayList<String>();
      lst.addAll(working_map.keySet());
      for(int i=0; i<lst.size()-1; i++)
      {
        String a = lst.get(i);
        String b = lst.get(i+1);
        NodeEntry ne_a = working_map.get(a).ne;
        NodeEntry ne_b = working_map.get(b).ne;
        WorkUnit wu_a = working_map.get(a);
        WorkUnit wu_b = working_map.get(b);

        // If b should go into a, and a is a node
        // just add it
        if ((b.startsWith(a)) && (ne_a.node))
        {
          TreeSet<String> rm_lst = new TreeSet<String>();
          
          for(int j=i+1; j<lst.size(); j++)
          {
            String c = lst.get(j);
            WorkUnit wu_c = working_map.get(c);
            NodeEntry ne_c = wu_c.ne;
            if (c.startsWith(a))
            {
              rm_lst.add(c);

              if (ne_c.node)
              { 
                if (ne_c.location == -1)
                {
                  //C must be a node we just added
                  Assert.assertEquals("C must be just added node", -1,ne_c.location);
                  wu_a.put_map.putAll(wu_c.put_map);
                }
                else
                { // C must be existing, add it as a sub
                  wu_a.put_map.put(c, ne_c);

                  // Anything we want to add to C should be added to this new common instead
                  wu_a.put_map.putAll(wu_c.put_map);
                }

              }
              else
              {
                wu_a.put_map.put(c, ne_c);
              }
            }
 
          }

          for(String s : rm_lst)
          {
            working_map.remove(s);
          }

          keep_looking=true;
          break;
                   

        }
        if (working_map.size() > 8)
        {

          int common = commonLength(stack, a, b) - prefix.length();
          if (common > 0)
          {
            String common_prefix = a.substring(0, common + prefix.length());

            TreeSet<String> rm_lst = new TreeSet<String>();

            WorkUnit wu_common = new WorkUnit(stack, common_prefix, save_entries);

            for(int j=i+1; j<lst.size(); j++)
            {
              String c = lst.get(j);
              if (c.startsWith(common_prefix))
              {
                WorkUnit wu_c = working_map.get(c);
                NodeEntry ne_c = wu_c.ne;
                if (ne_c.node)
                {
                  if (ne_c.location == -1)
                  {
                    //C must be a node we just added
                    Assert.assertEquals("C must be just added node", -1,ne_c.location);
                    wu_common.put_map.putAll(wu_c.put_map);
                  }
                  else
                  { // C must be existing, add it as a sub
                    wu_common.put_map.put(c, ne_c);

                    // Anything we want to add to C should be added to this new common instead
                    wu_common.put_map.putAll(wu_c.put_map);
                  }
                }
                else
                {
                  wu_common.put_map.put(c, ne_c);
                }
   
                rm_lst.add(c);
              }
            }

            for(String s : rm_lst)
            {
              working_map.remove(s);
            }
            working_map.put(common_prefix, wu_common);

            keep_looking=true;
            break;
          }
        }

      }
    }
  
    for(Map.Entry<String, WorkUnit> me : working_map.entrySet())
    {
      WorkUnit wu = me.getValue();
      wu.assertConsistentForPut();
      //Load node as needed
      if ((wu.ne.node) && (wu.put_map.size() > 0) && (wu.node==null))
      {
        wu.node = loadNode(stack, save_entries, wu.ne.location);
      }
      if (wu.put_map.size() > 0)
      {
        if (!stack.getQueue().offer(wu))
        {
          NodeEntry ne = wu.node.putAll(stack, wu.save_entries, wu.put_map);
          wu.return_entry.setResult(ne);
        }
      }
    }

    for(Map.Entry<String, WorkUnit> me : working_map.entrySet())
    {
      WorkUnit wu = me.getValue();
      if (wu.put_map.size() > 0)
      {
        children.put(me.getKey(), wu.return_entry.get());
      }
      else
      {
        children.put(me.getKey(), wu.ne);
      }
    }

    long t1_serialize = System.nanoTime();
    ByteBuffer self_buffer = serialize();
    tr.addTime(System.nanoTime() - t1_serialize, "serialize");

    long t1_comp = System.nanoTime();
    ByteBuffer comp = stack.compress(self_buffer);
    tr.addTime(System.nanoTime() - t1_comp, "compress");

    long t1_allocate = System.nanoTime();
    long self_loc = stack.allocateSpace(comp.capacity());
    tr.addTime(System.nanoTime() - t1_allocate, "alloc");

    synchronized(save_entries)
    {
      save_entries.put(self_loc, comp);
    }

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
    LobstackNode n = null;
    synchronized(save_entries)
    {
      if (save_entries.containsKey(location))
      {
        n = LobstackNode.deserialize(stack.decompress(save_entries.get(location)));
        throw new RuntimeException("whatfuck");
      }
    }
    long t1 = System.nanoTime();
    if (n == null)
    {
      n = stack.loadNodeAt(location);
    }

    stack.getTimeReport().addTime(System.nanoTime() - t1, "loadnode");
    return n;


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

  public static int commonLength(Lobstack stack, String a, String b)
  {
    int max = Math.min(a.length(), b.length());

    int same = 0;
    for(int i=0; i<max; i++)
    {
      if (a.charAt(i) == b.charAt(i)) same++;
      else break;
    }
    if (stack.key_step_size > 1)
    {
      same -= (same % stack.key_step_size);
    }
    return same;

  }


}

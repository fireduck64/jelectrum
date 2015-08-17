
package lobstack;


import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.text.DecimalFormat;

import java.io.File;
import java.nio.ByteBuffer;

import java.io.RandomAccessFile;
import jelectrum.LRUCache;
import java.io.IOException;
import java.util.Random;
import java.nio.channels.FileChannel;


/**
 * Limitations: 
 *  - Don't store null.  Zero length should be fine.
 *  - Don't store strings that end with ♥ (don't ask)
 *  - Puts lock things, try to use putAll as much as you can
 */
public class Lobstack
{
  public String DATA_TAG="♥";
  public long SEGMENT_FILE_SIZE=256L * 1024L * 1024L;

  public static final int MAX_OPEN_FILES=512;
  public static final int MAX_CACHED_DATA=32*1024;
  public static final int MAX_CACHE_SIZE=65536;

  public static final String MODE="rw";
  public static final boolean DEBUG=false;

  private Object ptr_lock = new Object();
  private long current_root;
  private long current_write_location;

  private File dir;
  private String stack_name;

  private FileChannel root_file_channel;

  private static long ROOT_ROOT_LOCATION = 0;
  private static long ROOT_WRITE_LOCATION = 8;

  private LRUCache<Long, FileChannel> data_files;
  private LRUCache<Long, ByteBuffer> cached_data;



  public Lobstack(File dir, String name)
    throws IOException
  {
    this.dir = dir;
    this.stack_name = name;

    if (!dir.exists())
    {
      throw new java.io.IOException("Directory does not exist: " + dir);
    }
    if (!dir.isDirectory())
    {
      throw new java.io.IOException("Location is not a directory: " + dir);
    }

    data_files = new LRUCache<Long, FileChannel>(MAX_OPEN_FILES);
    cached_data = new LRUCache<Long, ByteBuffer>(MAX_CACHED_DATA);

    RandomAccessFile root_file = new RandomAccessFile(new File(dir, name + ".root"), MODE);

    root_file_channel = root_file.getChannel();

    if (root_file.length()==0)
    {
      root_file.setLength(16);
      reset();
    }
    else
    {
      synchronized(ptr_lock)
      {
        root_file.seek(ROOT_ROOT_LOCATION);
        current_root = root_file.readLong();
        root_file.seek(ROOT_WRITE_LOCATION);
        current_write_location = root_file.readLong();
      }


    }
 
    synchronized(ptr_lock)
    {
      double gb = current_write_location / 1024.0 / 1024.0 / 1024.0;
      DecimalFormat df = new DecimalFormat("0.000");
      System.out.println(stack_name + ": GB: " + df.format(gb));
    }
  }

  private void reset()
    throws IOException
  {
    synchronized(cached_data)
    {
      cached_data.clear();
    }
    synchronized(ptr_lock)
    {
      current_root=-1;
      current_write_location=0;


      LobstackNode root = new LobstackNode("");
      ByteBuffer serial = root.serialize();
      long loc = allocateSpace(serial.capacity());
      TreeMap<Long, ByteBuffer> saves = new TreeMap<Long, ByteBuffer> ();
      saves.put(loc, serial);
      saveGroup(saves);
      setRoot(loc);
    }
    synchronized(cached_data)
    {
      cached_data.clear();
    }


  }

  /**
   * Compress the table to the entries that are currently visible.
   * All entries must fit in memory.
   * This breaks existing snapshots.
   * Something breaking during this will absolutely hose the database
   */ 
  public synchronized void compress()
    throws IOException
  {
    Map<String, ByteBuffer> all_data = getByPrefix("");
    reset();
    putAll(all_data);
    
  }

 


  public void put(String key, ByteBuffer data)
    throws IOException
  {
    TreeMap<String, ByteBuffer> put_map = new TreeMap<String, ByteBuffer>();
    put_map.put(key, data);
    putAll(put_map);
  }

  private long getCurrentRoot()
  {
    synchronized(ptr_lock)
    {
      return current_root;
    }

  }

  public void printTree()
    throws IOException
  {
    LobstackNode root = loadNodeAt(getCurrentRoot());
    root.printTree(this);

  }

  public synchronized void putAll(Map<String, ByteBuffer> put_map)
    throws IOException
  {
    LobstackNode root = loadNodeAt(getCurrentRoot());
    TreeMap<Long, ByteBuffer> save_entries=new TreeMap<Long, ByteBuffer>();

    TreeMap<String, NodeEntry> new_nodes = new TreeMap<String, NodeEntry>();

    for(String key : put_map.keySet())
    {
      ByteBuffer value = put_map.get(key);
      NodeEntry ne = new NodeEntry();
      ne.node=false;
      ne.location=allocateSpace(value.capacity());
      save_entries.put(ne.location, value);
      new_nodes.put(key + DATA_TAG, ne);
    }

    long new_root = root.putAll(this, save_entries, new_nodes);

    saveGroup(save_entries);

    setRoot(new_root);

  }
  public ByteBuffer get(String key)
    throws IOException
  {
    long root_loc = getCurrentRoot();
    return get(key, root_loc);

  }


  public ByteBuffer get(String key, long snapshot)
    throws IOException
  {
    LobstackNode root = loadNodeAt(snapshot);
    return root.get(this, key + DATA_TAG);

  }


  /**
   * Since this amazing waste of space keeps everything, you can just get a pointer
   * to a state and keep it wherever you like.
   *
   * The returned location can be used with revertSnapshot() which will revert the
   * tree to the state when the getSnapshot() was called.
   *
   * Or you can use the snapshot with get() or getByPrefix() to get data from that
   * particular version of the tree.  Sweet.
   */
  public long getSnapshot()
  {
    return getCurrentRoot(); 
  }
  public void revertSnapshot(long snapshot)
    throws IOException
  {
    setRoot(snapshot);
  }

  public SortedMap<String, ByteBuffer> getByPrefix(String prefix)
    throws IOException
  {
    long root_loc = getCurrentRoot();
    return getByPrefix(prefix, root_loc);
  }

  public SortedMap<String, ByteBuffer> getByPrefix(String prefix, long snapshot)
    throws IOException
  {
    LobstackNode root = loadNodeAt(snapshot);
    Map<String, ByteBuffer> data = root.getByPrefix(this, prefix);

    TreeMap<String, ByteBuffer> return_map = new TreeMap<String, ByteBuffer>();

    for(Map.Entry<String, ByteBuffer> me : data.entrySet())
    {
      String key = me.getKey();

      return_map.put( key.substring(0,key.length() - 1), me.getValue());
    }

    return return_map;

  }

  protected ByteBuffer loadAtLocation(long loc)
    throws IOException
  {
    synchronized(cached_data)
    {
      ByteBuffer bb = cached_data.get(loc);
      if (bb != null) return bb;
    }


    long file_idx = loc / SEGMENT_FILE_SIZE;
    long in_file_loc = loc % SEGMENT_FILE_SIZE;
    FileChannel fc = getDataFile(file_idx);
    ByteBuffer bb = null;
    synchronized(fc)
    {
      fc.position(in_file_loc);
      ByteBuffer lenbb = ByteBuffer.allocate(4);

      readBuffer(fc, lenbb);
      lenbb.rewind();


      int len = lenbb.getInt();

      byte[] buff = new byte[len];
      bb = ByteBuffer.wrap(buff);
      readBuffer(fc, bb);
    }
    
    if (bb.capacity() < MAX_CACHE_SIZE)
    {
      synchronized(cached_data)
      {
        cached_data.put(loc, bb);
      }
    }
    return bb;

  }

  protected LobstackNode loadNodeAt(long loc)
    throws IOException
  {
    ByteBuffer b = loadAtLocation(loc);
    return LobstackNode.deserialize(b);

  }


  protected long allocateSpace(int size)
    throws IOException
  {
    synchronized(ptr_lock)
    {
      synchronized(root_file_channel)
      {
        long loc = current_write_location;

        long new_end = loc + size + 4;
        //If this would go into the next segment, just go to next segment
        if ((loc / SEGMENT_FILE_SIZE) < (new_end / SEGMENT_FILE_SIZE))
        {
          loc = (new_end / SEGMENT_FILE_SIZE) * SEGMENT_FILE_SIZE;
        }

        current_write_location = loc + size + 4;
        root_file_channel.position(ROOT_WRITE_LOCATION);
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(current_write_location);
        bb.rewind();
        writeBuffer(root_file_channel, bb);
    
        return loc;
      }
    }

  }
  private void saveGroup(SortedMap<Long, ByteBuffer> save_entries)
    throws IOException
  {

    FileChannel last_fc = null;

    for(Map.Entry<Long, ByteBuffer> me : save_entries.entrySet())
    {
      long start_location = me.getKey();
      ByteBuffer data = me.getValue();
      int data_size = data.capacity();
      if (DEBUG) System.out.println(stack_name + " - saving to " + me.getKey() + " sz " + data_size);

      if (data_size < MAX_CACHE_SIZE)
      {
        synchronized(cached_data)
        {
          cached_data.put(start_location, data);
        }
      }

      long file_idx = start_location / SEGMENT_FILE_SIZE;

      long in_file_loc = start_location % SEGMENT_FILE_SIZE;

      FileChannel fc = getDataFile(file_idx);

      if ((last_fc != null) && (last_fc != fc))
      {
        synchronized(last_fc)
        {
          last_fc.force(true);
        }
      }

      synchronized(fc)
      {

        fc.position(in_file_loc);
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(data_size);
        bb.rewind();
        writeBuffer(fc,bb);
        writeBuffer(fc,data);

        last_fc = fc;
      }

    }

    if (last_fc != null)
    {
      synchronized(last_fc)
      {
        last_fc.force(true);
      }
    }


  }

  private void writeBuffer(FileChannel fc, ByteBuffer bb)
    throws IOException
  {
    while(bb.remaining()>0)
    {
      fc.write(bb);
    }
  }
  private void readBuffer(FileChannel fc, ByteBuffer bb)
    throws IOException
  {
    while(bb.remaining()>0)
    {
      fc.read(bb);
    }
  }

  private void setRoot(long loc)
    throws IOException
  {
  
    if (DEBUG) System.out.println(stack_name + " - new root at " + loc);
    synchronized(ptr_lock)
    {
      synchronized(root_file_channel)
      {
        root_file_channel.position(ROOT_ROOT_LOCATION);

        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(loc);
        bb.rewind();
        writeBuffer(root_file_channel, bb);

        root_file_channel.force(true);
 
        current_root = loc;
      }
    }
  }

  private FileChannel getDataFile(long idx)
    throws IOException
  {
    synchronized(data_files)
    {
      FileChannel fc = data_files.get(idx);
      if (fc == null)
      {
        String num = "" + idx;
        while(num.length() < 4) num = "0" + num;
        RandomAccessFile f = new RandomAccessFile(new File(dir, stack_name +"." + num + ".data"), MODE);

        f.setLength(SEGMENT_FILE_SIZE);

        fc = f.getChannel();

        data_files.put(idx,fc);
      }

      return fc;
    }
  }


}

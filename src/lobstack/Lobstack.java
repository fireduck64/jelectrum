
package lobstack;


import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.text.DecimalFormat;

import java.io.File;
import java.nio.ByteBuffer;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.Random;
import java.nio.channels.FileChannel;
import java.io.PrintStream;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import jelectrum.LRUCache;

/**
 * Limitations: 
 *  - Don't store null.  Zero length should be fine.
 *  - Don't store strings that end with ♥ (don't ask)
 *  - Puts lock things, try to use putAll as much as you can
 */
public class Lobstack
{
  public static String DATA_TAG="♥";
  public static long SEGMENT_FILE_SIZE=256L * 1024L * 1024L;

  public static final int MAX_OPEN_FILES=2048;
  public static final int MAX_CACHED_DATA=32*1024;
  public static final int MAX_CACHE_SIZE=65536;

  public static final String MODE="rw";
  public static final boolean DEBUG=false;

  private Object ptr_lock = new Object();
  private long current_root;
  private long current_write_location;
  

  private File dir;
  private String stack_name;
  private boolean compress;

  private FileChannel root_file_channel;

  private static long ROOT_ROOT_LOCATION = 0;
  private static long ROOT_WRITE_LOCATION = 8;

  private AutoCloseLRUCache<Long, FileChannel> data_files;
  private LRUCache<Long, ByteBuffer> cached_data;

  public Lobstack(File dir, String name)
    throws IOException
  {
    this(dir, name, false);
  }

  public Lobstack(File dir, String name, boolean compress)
    throws IOException
  {
    this.dir = dir;
    this.stack_name = name;
    this.compress = compress;

    if (!dir.exists())
    {
      throw new java.io.IOException("Directory does not exist: " + dir);
    }
    if (!dir.isDirectory())
    {
      throw new java.io.IOException("Location is not a directory: " + dir);
    }

    data_files = new AutoCloseLRUCache<Long, FileChannel>(MAX_OPEN_FILES);
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
 
    showSize();

  }
  public void showSize()
  {
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
      ByteBuffer com = compress(serial);
      long loc = allocateSpace(com.capacity());
      TreeMap<Long, ByteBuffer> saves = new TreeMap<Long, ByteBuffer> ();
      saves.put(loc, com);
      saveGroup(saves);
      setRoot(loc);
    }
    synchronized(cached_data)
    {
      cached_data.clear();
    }
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

  public synchronized void close()
    throws IOException
  {
    root_file_channel.force(true);
    root_file_channel.close();

    synchronized(data_files)
    {
      for(FileChannel fc : data_files.values())
      {
        fc.force(true);
        fc.close();
      }
    }
  
  }

  public void printTree()
    throws IOException
  {
    LobstackNode root = loadNodeAt(getCurrentRoot());
    root.printTree(this);

  }

  public TreeStat getTreeStats()
    throws IOException
  {
    LobstackNode root = loadNodeAt(getCurrentRoot());
    TreeStat stat = new TreeStat();
    root.getTreeStats(this, stat);
    return stat;

  }

  public void cleanup(double utilization, long max_move)
    throws IOException
  {
    cleanup(utilization, max_move, System.out);

  }
  public void cleanup(double utilization, long max_move, PrintStream out)
    throws IOException
  {
    TreeStat stat = getTreeStats();

    TreeMap<Integer, Long> file_use_map = stat.file_use_map;
    if (file_use_map.size() == 0) return;
    int repos = file_use_map.firstKey()-1;
    long move = 0;

    
    DecimalFormat df = new DecimalFormat("0.00");

    double found_util = ((stat.node_size + stat.data_size) * 1.0) / (file_use_map.size() * SEGMENT_FILE_SIZE);
    out.println(stack_name + ": utilization " + df.format(found_util));

    int max_pos = file_use_map.lastKey() - 4;

    if ((found_util < utilization) && (file_use_map.size() > 4))
    {
      for(int idx : file_use_map.keySet())
      {
        long sz = file_use_map.get(idx);
        if ((move + sz <= max_move) && (idx < max_pos))
        {
          repos = idx+1;
          move = move + sz;
        }
      }
    }

    double mb = move / 1024.0 / 1024.0;
    out.println(stack_name + ": repositioning to " + repos + " moving " + df.format(mb) + " mb");
    reposition(repos);
    out.println(stack_name + ": repositioning done");

  }

  /**
   * Reposition all data that is in files less than the given file number.
   * Will break existing snapshots.
   */
  public synchronized void reposition(int min_file)
    throws IOException
  {
    LobstackNode root = loadNodeAt(getCurrentRoot());

    TreeMap<Long, ByteBuffer> save_entries=new TreeMap<Long, ByteBuffer>();

    NodeEntry root_entry = root.reposition(this, save_entries, min_file);
    long new_root = root_entry.location;

    saveGroup(save_entries);

    setRoot(new_root);

    synchronized(data_files)
    {
      for(int idx = 0; idx< min_file; idx++)
      {
        FileChannel fc = data_files.get(idx);
        if (fc != null)
        {
          synchronized(fc)
          {
            data_files.remove(idx);
            fc.close();
          }
        }
      
        File f = getDataFile(idx);
        f.delete();
      }
    }



 
  }
  

  public void printTreeStats()
    throws IOException
  {
    getTreeStats().print();
  }

  public void getAll(BlockingQueue<Map.Entry<String, ByteBuffer> > consumer)
    throws IOException, InterruptedException
  {

    LobstackNode root = loadNodeAt(getCurrentRoot());
    root.getAll(this, consumer);
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
      ByteBuffer comp = compress(value);
      ne.location=allocateSpace(comp.capacity());
      ne.min_file_number = (int)(ne.location / SEGMENT_FILE_SIZE); 
      save_entries.put(ne.location, comp);
      new_nodes.put(key + DATA_TAG, ne);
    }

    NodeEntry root_entry = root.putAll(this, save_entries, new_nodes);
    long new_root = root_entry.location;

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
      ByteBuffer bb = me.getValue();
      bb.rewind();

      return_map.put( key.substring(0,key.length() - 1), bb);
    }

    return return_map;

  }

  protected int loadSizeAtLocation(long loc)
    throws IOException
  {

    long file_idx = loc / SEGMENT_FILE_SIZE;
    long in_file_loc = loc % SEGMENT_FILE_SIZE;
    FileChannel fc = getDataFileChannel(file_idx);
    ByteBuffer bb = null;
    synchronized(fc)
    {
      fc.position(in_file_loc);
      ByteBuffer lenbb = ByteBuffer.allocate(4);

      readBuffer(fc, lenbb);
      lenbb.rewind();


      int len = lenbb.getInt();
      return len;
    }


  }

  protected ByteBuffer loadAtLocation(long loc)
    throws IOException
  {
    synchronized(cached_data)
    {
      ByteBuffer bb = cached_data.get(loc);
      if (bb != null) return decompress(bb);
    }

    long file_idx = loc / SEGMENT_FILE_SIZE;
    long in_file_loc = loc % SEGMENT_FILE_SIZE;
    FileChannel fc = getDataFileChannel(file_idx);
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
      bb.rewind();
    }

    if (bb.capacity() < MAX_CACHE_SIZE)
    {
      synchronized(cached_data)
      {
        cached_data.put(loc, ByteBuffer.wrap(bb.array()));
      }
    }
    ByteBuffer de_bb = decompress(bb);
    if (DEBUG) System.out.println("Decompress");
    return de_bb;

  }

  protected LobstackNode loadNodeAt(long loc)
    throws IOException
  {
    ByteBuffer b = loadAtLocation(loc);
    return LobstackNode.deserialize(b);

  }

  protected ByteBuffer compress(ByteBuffer in)
  {
    if (!compress) return in;
    int sz = in.capacity();

    ByteBuffer c = ByteBuffer.wrap(ZUtil.compress(in.array()));
    if (DEBUG) System.out.println(" " + sz + " -> " + c.capacity());
    return c;
  }
  protected ByteBuffer decompress(ByteBuffer in)
  {
    if (!compress) return in;

    return ByteBuffer.wrap(ZUtil.decompress(in.array()));
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

      FileChannel fc = getDataFileChannel(file_idx);

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

        data.rewind();
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
    bb.rewind();
    while(bb.remaining()>0)
    {
      fc.write(bb);
    }
    bb.rewind();
  }
  private void readBuffer(FileChannel fc, ByteBuffer bb)
    throws IOException
  {
    bb.rewind();
    while(bb.remaining()>0)
    {
      fc.read(bb);
    }
    bb.rewind();
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

  private FileChannel getDataFileChannel(long idx)
    throws IOException
  {
    synchronized(data_files)
    {
      FileChannel fc = data_files.get(idx);
      if (fc == null)
      {
        RandomAccessFile f = new RandomAccessFile(getDataFile(idx), MODE);

        f.setLength(SEGMENT_FILE_SIZE);

        fc = f.getChannel();

        data_files.put(idx,fc);
      }

      return fc;
    }
  }

  private File getDataFile(long idx)
  {
    String num = "" + idx;
    while(num.length() < 4) num = "0" + num;

    return new File(dir, stack_name +"." + num + ".data");
  
  }

}

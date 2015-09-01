
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
import jelectrum.TimeRecord;
import java.util.concurrent.SynchronousQueue;
import java.text.SimpleDateFormat;

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
  public static final int MAX_CACHE_ENTRIES=128*1024;
  public static final int MAX_CACHE_SIZE=65536;

  public static final String MODE="rw";
  public static final boolean DEBUG=false;

  public static final long MAGIC_LOCATION_ZERO=Long.MAX_VALUE;

  public final int key_step_size;

  private Object ptr_lock = new Object();
  private long current_root;
  private long current_write_location;

  private TimeRecord time_record=new TimeRecord();
  

  private File dir;
  private String stack_name;
  private boolean compress;

  private FileChannel root_file_channel;

  private static final long ROOT_ROOT_LOCATION = 0;
  private static final long ROOT_WRITE_LOCATION = 8;
  public static final long WORKER_THREAD=64; 

  private AutoCloseLRUCache<Long, FileChannel> data_files;
  //private ThreadLocal<AutoCloseLRUCache<Long, FileChannel>>  read_data_files=new ThreadLocal<AutoCloseLRUCache<Long, FileChannel>>();
  private LRUCache<Long, ByteBuffer> cached_data;

  private static SynchronousQueue<WorkUnit> queue;

  private ByteBuffer BB_ZERO = ByteBuffer.wrap(new byte[0]);

  static {
    queue = new SynchronousQueue<WorkUnit>();
    for(int i=0; i<WORKER_THREAD; i++)
    {
      new LobstackWorkThread(queue).start();
    }
  }

  public Lobstack(File dir, String name)
    throws IOException
  {
    this(dir, name, false, 2);
  }

  public Lobstack(File dir, String name, boolean compress)
    throws IOException
  {
    this(dir, name, compress, 2);
  }


  public Lobstack(File dir, String name, boolean compress, int key_step_size)
    throws IOException
  {
    this.key_step_size  = key_step_size;
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
    cached_data = new LRUCache<Long, ByteBuffer>(MAX_CACHE_ENTRIES);

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
    long file_loc = 0;
    synchronized(ptr_lock)
    {
      file_loc = current_write_location / SEGMENT_FILE_SIZE;
    }
    int count =0;
    for(long i=0; i<=file_loc; i++)
    {
      File f = getDataFile(i);
      if (f.exists()) count++;
    }
    double sz = SEGMENT_FILE_SIZE * 1.0 * count;
    double gb = sz / 1024.0 / 1024.0 / 1024.0;

      DecimalFormat df = new DecimalFormat("0.000");
      System.out.println(stack_name + ": GB: " + df.format(gb));

  }

  protected SynchronousQueue<WorkUnit> getQueue()
  {
    return queue;
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

  public boolean cleanup(int max_back, double utilization, long max_move)
    throws IOException
  {
    return cleanup(max_back, utilization, max_move, System.out);
  }

  

  public boolean cleanup(int max_back, double utilization, long max_move, PrintStream out)
    throws IOException
  {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    out.println( sdf.format(new java.util.Date()) + " - " + stack_name + ": cleanup check");

    DecimalFormat df = new DecimalFormat("0.00");
    int start = getMinFileNumber();
    int end = getMaxFileNumber();

    int check_end = Math.min(start + max_back, end - 8);

    TreeMap<Integer, Long> estimate_map = estimateReposition(check_end);

    for(int i=start; i<=check_end; i++)
    {
      if (!estimate_map.containsKey(i)) estimate_map.put(i, 0L);
    }

    for(int i : estimate_map.descendingKeySet())
    {
      
      double freed = (i - start) * Lobstack.SEGMENT_FILE_SIZE;
      double move = estimate_map.get(i);
      double mb = move / 1024.0 / 1024.0;

      double util = move /freed;
      out.println(sdf.format(new java.util.Date()) + " - " + stack_name + ": a move to " + i + " would have utilization " + df.format(util) + " and move " + df.format(mb) + " mb");

      if ((move / freed) < utilization)
      {
        out.println(sdf.format(new java.util.Date()) + " - " +stack_name + ": repositioning to " + i + " moving " + df.format(mb) + " mb");
        reposition(i);
        out.println(sdf.format(new java.util.Date()) + " - " +stack_name + ": repositioning done");
        return true;
      }
 

    }

    return false;
  }

  public TreeMap<Integer, Long> estimateReposition(int max_file)
    throws IOException
  {
    LobstackNode root = loadNodeAt(getCurrentRoot());
    return root.estimateReposition(this, max_file);


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

  public void printTimeReport(PrintStream out)
  {
    out.println(stack_name + " - time report");
    time_record.printReport(out);
    time_record.reset();
  }
  public TimeRecord getTimeReport()
  {
    return time_record;
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
    long t1_put = System.nanoTime();
    LobstackNode root = loadNodeAt(getCurrentRoot());

    long t1_setup = System.nanoTime();
    TreeMap<Long, ByteBuffer> save_entries=new TreeMap<Long, ByteBuffer>();

    TreeMap<String, NodeEntry> new_nodes = new TreeMap<String, NodeEntry>();

    for(String key : put_map.keySet())
    {
      ByteBuffer value = put_map.get(key);
      NodeEntry ne = new NodeEntry();
      ne.node=false;
      if (value.capacity() == 0)
      {
        ne.min_file_number = Integer.MAX_VALUE;
        ne.location = MAGIC_LOCATION_ZERO;
      }
      else
      {
        ByteBuffer comp = compress(value);
        ne.location=allocateSpace(comp.capacity());
        ne.min_file_number = (int)(ne.location / SEGMENT_FILE_SIZE); 
        save_entries.put(ne.location, comp);
      }
      new_nodes.put(key + DATA_TAG, ne);
    }

    time_record.addTime(System.nanoTime() - t1_setup, "putSetup");
    long t1_dbput = System.nanoTime();
    NodeEntry root_entry = root.putAll(this, save_entries, new_nodes);
    time_record.addTime(System.nanoTime() - t1_dbput, "putTreeWork");

    long new_root = root_entry.location;

    long t1_save = System.nanoTime();
    saveGroup(save_entries);

    setRoot(new_root);
    time_record.addTime(System.nanoTime() - t1_save, "putSave");

    time_record.addTime(System.nanoTime() - t1_put, "putAll");

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

  public int getMinFileNumber()
    throws IOException
  {
    int idx = 0;

    while(true)
    { 
      File f = getDataFile(idx);
      if (f.exists()) return idx;
    }

    /*long root_loc = getCurrentRoot();
    LobstackNode root = loadNodeAt(root_loc);
    return root.getMinFileNumber(root_loc);*/
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
    if (loc == MAGIC_LOCATION_ZERO) return 0;

    long file_idx = loc / SEGMENT_FILE_SIZE;
    long in_file_loc = loc % SEGMENT_FILE_SIZE;
    try(FileChannel fc = getDataFileChannelRead(file_idx))
    {
      ByteBuffer bb = null;
    
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
    if (loc == MAGIC_LOCATION_ZERO) return BB_ZERO;

    synchronized(cached_data)
    {
      ByteBuffer bb = cached_data.get(loc);
      if (bb != null) return decompress(bb);
    }

    long file_idx = loc / SEGMENT_FILE_SIZE;
    long in_file_loc = loc % SEGMENT_FILE_SIZE;
    ByteBuffer bb = null;
    try(FileChannel fc = getDataFileChannelRead(file_idx))
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

  protected int getMaxFileNumber()
  {
    synchronized(ptr_lock)
    {
      return (int)(current_write_location / SEGMENT_FILE_SIZE);
    }
  }

  protected long allocateSpace(int size)
    throws IOException
  {
    if (size==0) return MAGIC_LOCATION_ZERO;

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
  private FileChannel getDataFileChannelRead(long idx)
    throws IOException
  {
      FileChannel fc;
      RandomAccessFile f = new RandomAccessFile(getDataFile(idx), "r");
      fc = f.getChannel();
      return fc;
  }


  private File getDataFile(long idx)
  {
    String num = "" + idx;
    while(num.length() < 4) num = "0" + num;

    return new File(dir, stack_name +"." + num + ".data");
  
  }

}

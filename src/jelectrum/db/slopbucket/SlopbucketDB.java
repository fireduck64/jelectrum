
package jelectrum.db.slopbucket;
import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;

import slopbucket.Slopbucket;
import jelectrum.Config;
import jelectrum.EventLog;
import jelectrum.DaemonThreadFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.TreeMap;
import com.google.protobuf.ByteString;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


public class SlopbucketDB extends DB
{


  private ThreadPoolExecutor exec;
  private EventLog log;

  private TreeMap<String, Slopbucket> slops;
  private File dir;



  public SlopbucketDB(Config conf, EventLog log)
    throws Exception
  {
    super(conf);
    this.log = log;

    conf.require("slopbucket_path");

    dir = new File(conf.get("slopbucket_path"));
    dir.mkdirs();

    slops=new TreeMap<>();

    exec = new ThreadPoolExecutor(
      16,
      16,
      2, TimeUnit.DAYS,
      new LinkedBlockingQueue<Runnable>(),
      new DaemonThreadFactory());

    open();
  }

  protected Executor getExec(){return exec;}


  protected DBMap openMap(String name) throws Exception
  {
    Slopbucket slop = getBucketForName(name);
    slop.addTrough(name);

    boolean comp = false;
    if (name.equals("block_map")) comp=true;
    return new SlopbucketMap(this, name, slop, comp);
  }

  protected DBMapSet openMapSet(String name) throws Exception
  {
    Slopbucket slop = getBucketForName(name);
    slop.addTrough(name);

    return new SlopbucketMapSet(this, name, slop);
  }

  protected Slopbucket getBucketForName(String name)
  {
    synchronized(slops)
    {
      if (slops.containsKey(name)) return slops.get(name);
      File subdir = new File(dir, name);
      subdir.mkdirs();
      Slopbucket slop = new Slopbucket(subdir, log);
      slops.put(name, slop);
      return slop;
    }

  }

  /*protected Slopbucket getBucketForKey(ByteString key)
  {
    int h = Math.abs(key.hashCode() % SLOP_COUNT);
    if (h < 0) h = 0;
    return slops.get(h);
  }*/

  protected EventLog getLog() { return log;}


}

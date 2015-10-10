
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
import com.google.protobuf.ByteString;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


public class SlopbucketDB extends DB
{

  private static final int SLOP_COUNT=16;

  private ThreadPoolExecutor exec;
  private EventLog log;


  private ArrayList<Slopbucket> slops;

  public SlopbucketDB(Config conf, EventLog log)
    throws Exception
  {
    super(conf);
    this.log = log;

    conf.require("slopbucket_path");

    File dir = new File(conf.get("slopbucket_path"));
    dir.mkdirs();

    slops=new ArrayList<>();

    for(int i=0; i<SLOP_COUNT; i++)
    {
      File subdir = new File(dir, "sub_" + i);
      subdir.mkdirs();

      Slopbucket slop = new Slopbucket(subdir, log);
      slops.add(slop);
    }

    exec = new ThreadPoolExecutor(
      SLOP_COUNT*2,
      SLOP_COUNT*2,
      2, TimeUnit.DAYS,
      new LinkedBlockingQueue<Runnable>(),
      new DaemonThreadFactory());

    open();
  }

  protected Executor getExec(){return exec;}


  protected DBMap openMap(String name) throws Exception
  {
    for(Slopbucket slop : slops)
    {
      slop.addTrough(name);
    }
    boolean comp = false;
    if (name.equals("block_map")) comp=true;
    return new SlopbucketMap(this, name, comp);
  }

  protected DBMapSet openMapSet(String name) throws Exception
  {
    for(Slopbucket slop : slops)
    {
      slop.addTrough(name);
    }
    return new SlopbucketMapSet(this, name);
  }

  protected Slopbucket getBucketForKey(ByteString key)
  {
    int h = Math.abs(key.hashCode() % SLOP_COUNT);
    if (h < 0) h = 0;
    return slops.get(h);
  }

  protected EventLog getLog() { return log;}


}

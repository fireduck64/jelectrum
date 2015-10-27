
package jelectrum.db.slopbucket;
import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;

import slopbucket.Slopbucket;
import jelectrum.Config;
import jelectrum.EventLog;
import java.io.File;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import com.google.protobuf.ByteString;


public class SlopbucketDB extends DB
{


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

    open();
  }


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
    throws Exception
  {
    synchronized(slops)
    {
      if (slops.containsKey(name)) return slops.get(name);
      File subfile = new File(dir, name +".slop");
      Slopbucket slop = new Slopbucket(subfile, log);
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



  @Override
  protected void dbShutdownHandler()
    throws Exception
  {
    log.alarm("Slopbucket: flushing");
    for(Map.Entry<String, Slopbucket> me : slops.entrySet())
    {
      String name = me.getKey();
      Slopbucket bucket = me.getValue();
      log.log("Slopbucket: flushing " + name);
      bucket.flush(true);
    }
    log.alarm("Slopbucket: Complete");
  }
        

}


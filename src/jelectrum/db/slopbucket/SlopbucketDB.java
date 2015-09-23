
package jelectrum.db.slopbucket;
import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;

import slopbucket.Slopbucket;
import jelectrum.Config;
import jelectrum.EventLog;
import java.io.File;
import java.util.ArrayList;
import com.google.protobuf.ByteString;

public class SlopbucketDB extends DB
{

  private static final int SLOP_COUNT=16;

  private ArrayList<Slopbucket> slops;

  public SlopbucketDB(Config conf, EventLog log)
    throws Exception
  {
    super(conf);

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



    open();
  }


  protected DBMap openMap(String name) throws Exception
  {
    for(Slopbucket slop : slops)
    {
      slop.addTrough(name);
    }
    return new SlopbucketMap(this, name);
  }


  protected DBMapSet openMapSet(String name) throws Exception{return null;}

  protected Slopbucket getBucketForKey(ByteString key)
  {
    int h = Math.abs(key.hashCode() % SLOP_COUNT);
    if (h < 0) h = 0;
    return slops.get(h);


  }


}

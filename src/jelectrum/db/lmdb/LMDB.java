package jelectrum.db.lmdb;


import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;

import jelectrum.Config;

import org.fusesource.lmdbjni.Env;

import java.io.File;

public class LMDB extends DB
{
  Env env;
  public LMDB(Config config)
    throws Exception
  {
    super(config);

    config.require("lmdb_path");
    String path = config.get("lmdb_path");

    File dir = new File(path);
    dir.mkdirs();

    env = new Env();
    env.setMaxDbs(16);
    env.setMapSize(500 * 1024L * 1024L * 1024L);
    env.open(path);

    open();

  }


  public DBMap openMap(String name)
  {
    return new LMDBMap(env, env.openDatabase(name));
  }
  public DBMapSet openMapSet(String name)
  {
    return new LMDBMapSet(env, env.openDatabase(name));
  }






}

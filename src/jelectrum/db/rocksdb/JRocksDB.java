package jelectrum.db.rocksdb;

import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.db.DBMapMutationSet;
import jelectrum.Config;
import jelectrum.EventLog;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.FlushOptions;


public class JRocksDB extends DB
{
  private RocksDB db;
  private Options options;
  private EventLog log;

  public JRocksDB(Config config, EventLog log)
    throws Exception
  {
    super(config);

    this.log = log;

    config.require("rocksdb_path");

    String path = config.get("rocksdb_path");

    RocksDB.loadLibrary();
    Options options = new Options();

    options.setIncreaseParallelism(16);
    options.setCreateIfMissing(true);
    options.setAllowMmapReads(true);
    //options.setAllowMmapWrites(true);

    db = RocksDB.open(options, path);

    open();
  }

  protected DBMapMutationSet openMutationMapSet(String name) throws Exception
  {
    return new RocksDBMapMutationSet(getExec(), db, name);
  }

  protected DBMap openMap(String name) throws Exception
  {
    return new RocksDBMap(getExec(), db, name);
  }
  protected DBMapSet openMapSet(String name) throws Exception
  {
    return new RocksDBMapSet(getExec(), db, name);
  }


  
  @Override
  protected void dbShutdownHandler()
    throws Exception
  {
    log.alarm("RocksDB: flushing");
    FlushOptions fl = new FlushOptions();
    fl.setWaitForFlush(true);
    db.flush(fl);
    log.alarm("RocksDB: flush complete");


  }





}

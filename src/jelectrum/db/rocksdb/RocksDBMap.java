package jelectrum.db.rocksdb;

import jelectrum.db.DBMap;
import jelectrum.db.DBMapThreaded;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import java.util.Map;

import java.util.concurrent.Executor;

import com.google.protobuf.ByteString;


public class RocksDBMap extends DBMapThreaded
{
  RocksDB db;
  String name;

  public RocksDBMap(Executor exec, RocksDB db, String name)
  {
    super(exec);
    this.db = db;
    this.name = name;
  }

  public ByteString get(String key)
  {
    String key_str = name + "/" + key;

    try
    {

      byte[] r = db.get(key_str.getBytes());
      if (r == null) return null;

      return ByteString.copyFrom(r);

    }
    catch(RocksDBException e)
    {
      throw new RuntimeException(e);
    }

  }

  public void put(String key, ByteString value)
  {
    try
    {
      String key_str = name + "/" + key;

      db.put(key_str.getBytes(), value.toByteArray());

    }
    catch(RocksDBException e)
    {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putAll(Map<String, ByteString> m)
  {
    try
    {
      WriteBatch batch = new WriteBatch();

      for(Map.Entry<String, ByteString> e : m.entrySet())
      {
        String key_str = name + "/" + e.getKey();
        batch.put(key_str.getBytes(), e.getValue().toByteArray());

      }

      WriteOptions write_options = new WriteOptions();
      write_options.setDisableWAL(true);
      write_options.setSync(false);

      db.write(write_options, batch);

    }
    catch(RocksDBException e)
    {
      throw new RuntimeException(e);
    }

  }


}

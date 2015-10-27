package jelectrum.db.rocksdb;

import jelectrum.db.DBMap;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import java.util.Map;

import com.google.protobuf.ByteString;


public class RocksDBMap extends DBMap
{
  RocksDB db;
  String name;

  public RocksDBMap(RocksDB db, String name)
  {
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

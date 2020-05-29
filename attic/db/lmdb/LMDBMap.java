package jelectrum.db.lmdb;

import java.util.Map;
import jelectrum.db.DBMap;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;
import com.google.protobuf.ByteString;

public class LMDBMap extends DBMap
{
  private Env env;
  private Database db;
  public LMDBMap(Env env, Database db)
  {
    this.env = env;
    this.db = db;
  }

  public ByteString get(String key)
  {
    byte[] key_data = key.getBytes();
    if (key_data.length == 0) key_data = new byte[1];


    byte[] b = db.get(key_data);
    if (b == null) return null;

    return ByteString.copyFrom(b);
  }
  public void put(String key, com.google.protobuf.ByteString value)
  {
    byte[] key_data = key.getBytes();
    if (key_data.length == 0) key_data = new byte[1];
    db.put(key_data, value.toByteArray());

  }
  public void putAll(Map<String, ByteString> m)
  {
    Transaction tx = env.createTransaction();

    for(Map.Entry<String, ByteString> me : m.entrySet())
    {
      String key = me.getKey();
      byte[] key_data = key.getBytes();
      if (key_data.length == 0) key_data = new byte[1];

      db.put(tx, key_data, me.getValue().toByteArray());
    }
    tx.commit();

  }


}

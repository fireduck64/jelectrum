package jelectrum.db.rocksdb;

import jelectrum.db.DBTooManyResultsException;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import jelectrum.db.DBMapMutationSet;
import jelectrum.db.DBMapMutationSetThreaded;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.Executor;

public class RocksDBMapMutationSet extends DBMapMutationSetThreaded
{
  JRocksDB jdb;
  RocksDB db;
  String name;
  byte[] name_bytes;
  byte sep = '/';
  public RocksDBMapMutationSet(JRocksDB jdb, Executor exec, RocksDB db, String name)
  {
    super(exec);
    this.db = db;
    this.name = name;
    this.jdb = jdb;
    name_bytes = name.getBytes();
  }

  private ByteString getDBKey(ByteString key, ByteString value)
  {
		try
		{
      ByteString.Output out = ByteString.newOutput(100);
      out.write(name_bytes);
      out.write(key.toByteArray());
      out.write(sep);	
      out.write(value.toByteArray());
      ByteString w = out.toByteString();
      return w;
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }
  }
  private ByteString getDBKey(ByteString key)
  {
    try
    {
      ByteString.Output out = ByteString.newOutput(100);
      out.write(name_bytes);
      out.write(key.toByteArray());
      out.write(sep);
      ByteString w = out.toByteString();
      return w;
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }
  }


  public void add(ByteString key, ByteString value)
  {
    byte b[]=new byte[0];

    ByteString w = getDBKey(key, value);
  
    try
    {
    	db.put(jdb.getWriteOption(), w.toByteArray(), b);
		}
    catch(RocksDBException e)
    {
      throw new RuntimeException(e);
    }

  }

  public void remove(ByteString key, ByteString value)
  {
		try
		{
    	ByteString w = getDBKey(key, value);
    	db.remove(jdb.getWriteOption(), w.toByteArray());

		}
    catch(RocksDBException e)
    {
      throw new RuntimeException(e);
    }
  }

  public Set<ByteString> getSet(ByteString key, int max_reply)
  {
		ByteString dbKey = getDBKey(key);

    HashSet<ByteString> set = new HashSet<>();
    int count = 0;
    RocksIterator it = db.newIterator();

    try
    {
      it.seek(dbKey.toByteArray());

      while(it.isValid())
      {
				ByteString curr_key = ByteString.copyFrom(it.key());
        if (!curr_key.startsWith(dbKey)) break;

        ByteString v = curr_key.substring(dbKey.size());
        set.add(v);
        count++;

        if (count > max_reply) throw new DBTooManyResultsException();

        it.next();

      }

    }
    finally
    {
      it.dispose();
    }


    return set;

  }

}

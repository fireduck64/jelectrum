package jelectrum.db.slopbucket;

import jelectrum.db.DBMapSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;

import org.bitcoinj.core.Sha256Hash;
import com.google.protobuf.ByteString;

import slopbucket.Slopbucket;
import java.util.concurrent.Semaphore;
import jelectrum.db.DBTooManyResultsException;



public class SlopbucketMapSet extends DBMapSet
{
  private SlopbucketDB slop_db;
  private String name;
  private Slopbucket slop_fixed;

  public SlopbucketMapSet(SlopbucketDB slop_db, String name, Slopbucket slop_fixed)
  {
    this.slop_db = slop_db;
    this.name = name;
    this.slop_fixed = slop_fixed;
  }


  public void add(String key, Sha256Hash hash)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());

    Slopbucket slop = slop_fixed;
    //if (slop == null) slop = slop_db.getBucketForKey(key_bytes);

    ByteString hash_bytes = ByteString.copyFrom(hash.getBytes());

    slop.addListEntry(name, key_bytes, hash_bytes);

  }
  public Set<Sha256Hash> getSet(String key, int max_results)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());

    Slopbucket slop = slop_fixed;
    //if (slop == null) slop = slop_db.getBucketForKey(key_bytes);
    
    Set<ByteString> entries = slop.getList(name, key_bytes);

    Set<Sha256Hash> set = new HashSet<Sha256Hash>();

    int count =0;

    for(ByteString bs : entries)
    {
      Sha256Hash h = new Sha256Hash(bs.toByteArray());
      set.add(h);
      count ++;

      if (count > max_results) throw new DBTooManyResultsException();
    }

    return set;
  }

  /*@Override
  public void addAll(Collection<Map.Entry<String, Sha256Hash> > lst)
  {
    HashMap<String, List<Sha256Hash> > map_view = new HashMap<>();

    for(Map.Entry<String, Sha256Hash> me : lst)
    {
      String key = me.getKey();
      Sha256Hash h = me.getValue();
      if (!map_view.containsKey(key)) map_view.put(key, new LinkedList<Sha256Hash>());

      map_view.get(key).add(h);
    }

    final Semaphore sem = new Semaphore(0);
    int count = 0;
    for(Map.Entry<String, List<Sha256Hash> > me : map_view.entrySet())
    {
      final String key = me.getKey();
      final List<Sha256Hash> kl = me.getValue();

      slop_db.getExec().execute(
        new Runnable()
        {
          public void run()
          {
            for(Sha256Hash h : kl)
            {
              add(key, h);
            }
            sem.release(1);
          }

        }
        );
      count++;
    }
    try
    {
      sem.acquire(count);
    }
    catch(InterruptedException e)
    {
      throw new RuntimeException(e);
    }

  }*/

}

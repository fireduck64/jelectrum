package jelectrum.db.lmdb;

import jelectrum.db.DBMapSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.bitcoin.core.Sha256Hash;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;


public class LMDBMapSet extends DBMapSet
{
  private Database db;
  private Env env;
  public LMDBMapSet(Env env, Database db)
  {
    this.db = db;
    this.env = env;
  }
  
  public void add(String key, Sha256Hash hash)
  {
    String k = key + "/" + hash.toString();

    db.put(k.getBytes(), new byte[1]);

  }
  public void addAll(Collection<Map.Entry<String, Sha256Hash> > lst)
  {
    Transaction tx = env.createTransaction();
    byte[] b=new byte[1];
    for(Map.Entry<String, Sha256Hash> me : lst)
    {
      String k = me.getKey() +"/" + me.getValue().toString();
      db.put(tx, k.getBytes(), b);
    }
    tx.commit();
  }


  public Set<Sha256Hash> getSet(String key)
  {
    String k = key + "/";
    Transaction tx = env.createTransaction(true);

    EntryIterator i = db.seek(tx, k.getBytes());

    Set<Sha256Hash> out = new TreeSet<Sha256Hash>();

    while(i.hasNext())
    {
      Entry e = i.next();
      String s = new String(e.getKey());
      if (s.startsWith(k))
      {
        String h = s.substring(k.length());
        out.add(new Sha256Hash(h));
      }
    }
    tx.abort();

    return out;

  }


}

package jelectrum.db.cassandra;

import jelectrum.db.DBMapSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.bitcoinj.core.Sha256Hash;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import java.util.Map;
import java.util.LinkedList;
import com.datastax.driver.core.ResultSetFuture;



import jelectrum.db.DBTooManyResultsException;

public class CassandraMapSet extends DBMapSet
{

  private Session session;
  private String tableName;



  public CassandraMapSet(Session session, String tableName)
  { 
    this.session = session;
    this.tableName = tableName;
    session.execute("CREATE TABLE IF NOT EXISTS "+tableName+" (key varchar, value varchar, primary key((key), value))");
  }


  public void add(String key, Sha256Hash hash)
  {
    session.execute(session.prepare("INSERT into "+tableName+" (key,value) values (?,?)").bind(key, hash.toString()));
  }
  public Set<Sha256Hash> getSet(String key, int max_reply)
  {
    ResultSet rs = session.execute(session.prepare("select value from "+tableName+" where key=?").bind(key));

    Set<Sha256Hash> ret = new HashSet<Sha256Hash>();

    int count =0;
    for(Row r : rs)
    {
      Sha256Hash hash = new Sha256Hash(r.getString(0));
      ret.add(hash);
      count ++;
      if (count > max_reply) throw new DBTooManyResultsException();
    }

    return ret;

  }

  public void addAll(Collection<Map.Entry<String, Sha256Hash> > lst)
  {
    LinkedList<ResultSetFuture> futureList = new LinkedList<>();

    BatchStatement bs = null;
    int bs_count = 0;

    int batch_size=20;

    for(Map.Entry<String, Sha256Hash> me : lst)
    { 
      String key = me.getKey();
      Sha256Hash hash = me.getValue();

      Statement s = session.prepare("INSERT into "+tableName+" (key,value) values (?,?)").bind(key, hash.toString());

      if (batch_size<=1)
      { 
        ResultSetFuture f = session.executeAsync(s);
        futureList.add(f);
      }
      else
      { 
        if (bs ==null)
        { 
          bs = new BatchStatement();
        }
        bs.add(s);
        bs_count++;

        if (bs_count >= batch_size)
        { 
          ResultSetFuture f = session.executeAsync(bs);
          futureList.add(f);
          bs=null;
          bs_count=0;
        }
      }

    }
    if (bs!=null)
    { 
      ResultSetFuture f = session.executeAsync(bs);
      futureList.add(f);
    }

    for(ResultSetFuture f : futureList)
    {
      f.getUninterruptibly();
    }
  
  }

}

package jelectrum.db.cassandra;

import jelectrum.db.DBMap;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.protobuf.ByteString;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import java.util.Map;
import java.util.LinkedList;
import com.datastax.driver.core.ResultSetFuture;

public class CassandraMap extends DBMap
{
  private Session session;
  private String tableName;

  public CassandraMap(Session session, String tableName)
  {
    this.session = session;
    this.tableName = tableName;
    session.execute("CREATE TABLE IF NOT EXISTS "+tableName+" (key varchar primary key, data blob);");
  }

  public ByteString get(String key)
  { 
    if (key.equals("")) key="____EMPTY______";

    ResultSet rs = session.execute(session.prepare("SELECT data from "+tableName+" where key=?").bind(key));

    Row r = rs.one();
    if (r == null) return null;

    return ByteString.copyFrom(r.getBytes(0));

  }
  public void put(String key, ByteString value)
  {
    if (key.equals("")) key="____EMPTY______";
    session.execute(session.prepare("UPDATE "+tableName+" set data=? where key=?").bind(value.asReadOnlyByteBuffer(), key));

  }
  public void putAll(Map<String, ByteString> m)
  {
    LinkedList<ResultSetFuture> futureList = new LinkedList<>();

    BatchStatement bs = null;
    int bs_count =0;
    long batch_bytes=0;

    int batch_size=1;
    long batch_max_bytes=50000;

    for(Map.Entry<String, ByteString> me : m.entrySet())
    {
      String key = me.getKey();
      ByteString value = me.getValue();
      long write_size =  key.length() + value.size();

      if (key.equals("")) key="____EMPTY______";

      Statement s = session.prepare("UPDATE "+tableName+" set data=? where key=?").bind(value.asReadOnlyByteBuffer(), key);
      if ((batch_size<=1) || (write_size > batch_max_bytes))
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
        batch_bytes+=write_size;

        if ((bs_count >= batch_size) || (batch_bytes >= batch_max_bytes))
        {
          ResultSetFuture f = session.executeAsync(bs);
          futureList.add(f);
          bs=null;
          bs_count=0;
          batch_bytes=0;
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

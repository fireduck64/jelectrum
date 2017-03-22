package jelectrum.db.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import java.nio.ByteBuffer;
import java.util.Random;

public class CassTest
{
  public static void main(String args[]) throws Exception
  {
    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

    Session session = cluster.connect("demo");

    byte[] data = new byte[86];

    Random rnd = new Random();
    rnd.nextBytes(data);

    {

    PreparedStatement ps = session.prepare("UPDATE maptable set data=? where key=?");
    session.execute(ps.bind(ByteBuffer.wrap(data), "zing"));
    }

    session.execute(session.prepare("INSERT into mapset (key,value) values (?,?)").bind("a","b" + rnd.nextInt()));



    cluster.close();

  }

}

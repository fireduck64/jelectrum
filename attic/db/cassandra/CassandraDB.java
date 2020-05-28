package jelectrum.db.cassandra;

import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.Config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;


public class CassandraDB extends jelectrum.db.DB
{
  Cluster cluster;
  Session session;

  public CassandraDB(Config config) throws Exception
  {
    super(config);
    conf.require("cassandra_keyspace");
    conf.require("cassandra_host");

    cluster = Cluster.builder().addContactPoint(conf.get("cassandra_host")).build();
    session = cluster.connect(conf.get("cassandra_keyspace"));
    

    open();

  }

  protected DBMap openMap(String name) throws Exception
  {
    return new CassandraMap(session, name);

  }

  protected DBMapSet openMapSet(String name) throws Exception
  {
    return new CassandraMapSet(session, name);
  }


}

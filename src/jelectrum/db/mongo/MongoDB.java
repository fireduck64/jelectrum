package jelectrum.db.mongo;
import com.datastax.driver.core.Session;

import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.Config;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.DBCollection;
import com.mongodb.ServerAddress;


public class MongoDB extends jelectrum.db.DB
{
  private MongoClient mc;
  private com.mongodb.DB db;

  public MongoDB(Config config)
    throws Exception
  {
    super(config);

    conf.require("mongo_db_host");
    conf.require("mongo_db_name");
    conf.require("mongo_db_connections_per_host");

    MongoClientOptions.Builder opts = MongoClientOptions.builder();
    opts.connectionsPerHost(conf.getInt("mongo_db_connections_per_host"));
    opts.threadsAllowedToBlockForConnectionMultiplier(100);
    opts.socketTimeout(3600000);


    mc = new MongoClient(new ServerAddress(conf.get("mongo_db_host")), opts.build());

    db = mc.getDB(conf.get("mongo_db_name"));


    open();
  }
  
  protected DBMap openMap(String name) throws Exception
  {
    DBCollection c = db.getCollection(name);
    return new MongoMap(c);

    
  }
  protected DBMapSet openMapSet(String name) throws Exception
  {
    DBCollection c = db.getCollection(name);
    return new MongoMapSet(c);

  }
}

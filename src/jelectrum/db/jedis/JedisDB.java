package jelectrum.db.jedis;

import jelectrum.db.DB;
import jelectrum.Config;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisDB extends DB
{
  private JedisPool pool;
  private String db_name;

  public JedisDB(Config config)
    throws Exception
  {
    super(config);

    config.require("redis_db_name");
    db_name = config.get("redis_db_name");

    JedisPoolConfig pconf = new JedisPoolConfig();

    if (config.isSet("redis_max_connections"))
    {
      pconf.setMaxTotal(config.getInt("redis_max_connections"));
    }

    pool = new JedisPool(pconf, "localhost");
    System.out.println("Redis max connections: " + pconf.getMaxTotal());

    open();
  }
  protected DBMap openMap(String name) throws Exception
  {
    return new JedisMap(pool, db_name + "/" + name);

  }
  protected DBMapSet openMapSet(String name) throws Exception
  {
    return new JedisMapSet(pool, db_name + "/" + name);
  }



}

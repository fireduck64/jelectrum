package jelectrum.db.jedis;

import com.google.protobuf.ByteString;
import java.util.Map;

import jelectrum.db.DBMapSet;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;


import org.bitcoinj.core.Sha256Hash;
import java.util.HashSet;
import java.util.Set;


public class JedisMapSet extends DBMapSet
{
  private JedisPool pool;
  private String name;

  public JedisMapSet(JedisPool pool, String name)
  {
    this.pool = pool;
    this.name = name;
  }


  @Override
  public void add(String key, Sha256Hash hash)
  {
    String look = name + "/" + key;
    try(Jedis jedis = pool.getResource())
    {
      jedis.sadd(look.getBytes(), hash.getBytes());
    }

  }

  @Override
  public Set<Sha256Hash> getSet(String key, int max_reply)
  {
    String look = name + "/" + key;
    Set<Sha256Hash> set = new HashSet<>();

    try(Jedis jedis = pool.getResource())
    {
      if (jedis.scard(look) > max_reply)
      {
        throw new jelectrum.db.DBTooManyResultsException();
      }
      Set<byte[]> smembers = jedis.smembers(look.getBytes());
      for(byte[] b : smembers)
      {
        Sha256Hash h = new Sha256Hash(b);
        set.add(h);
      }
    }


    return set;

    

  }

}

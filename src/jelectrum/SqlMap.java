package jelectrum;

import java.util.Map;
import java.util.Set;
import java.util.Collection;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.Random;
import java.text.DecimalFormat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;

public class SqlMap<K,V> implements Map<K, V>
{
    private String table_name;

    private static StatData put_stats = new StatData();
    private static StatData get_stats = new StatData();



    public SqlMap(String table_name, int max_key_len)
    {
      this.table_name = table_name;
      createTable(max_key_len);
    }

    public void createTable(int max_key_len)
    {
      Connection conn = null;
      try
      {
        conn = DB.openConnection("jelectrum_db");
        PreparedStatement ps = conn.prepareStatement("create table "+table_name+" ( key char("+max_key_len+") PRIMARY KEY, data BYTEA)");
        ps.execute();
        ps.close();

      }
      catch(SQLException e)
      {
      }
      finally
      {
        DB.safeClose(conn);
      }

    }

    public void clear()
    {
      while(true)
      {
        try
        {
          tryClear();
          return;
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }

      }

    }

    private void tryClear() throws SQLException
    {
      Connection conn = null;
      try
      {
        conn = DB.openConnection("jelectrum_db");
        PreparedStatement ps = conn.prepareStatement("delete from "+table_name);
        ps.execute();
        ps.close();

      }
      finally
      {
        DB.safeClose(conn);
      }
    }


    public boolean containsKey(Object key)
    {
      while(true)
      {
        try
        {
          return tryContainsKey(key);
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }

      }

    }



    private boolean tryContainsKey(Object key) throws SQLException
    {
        long t1 = System.currentTimeMillis();
        boolean c = false;
        Connection conn = null;
        try
        {
          conn = DB.openConnection("jelectrum_db");
          PreparedStatement ps = conn.prepareStatement("select count(key) as count from "+table_name+" where key=?");
          ps.setString(1, key.toString());
          ResultSet rs = ps.executeQuery();
          rs.next();

          int count = rs.getInt("count");
          if (count > 0) c = true;

          rs.close();
          ps.close();

        }
        finally
        {
          DB.safeClose(conn);
        }

        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);
        return c;
    }

    public boolean containsValue(Object value)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public  boolean equals(Object o)
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public V get(Object key)
    {
      while(true)
      {
        try
        {
          return tryGet(key);
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }
        catch(IOException e)
        {
          throw new RuntimeException(e);
        }
        catch(ClassNotFoundException e)
        {
          throw new RuntimeException(e);
        }

      }
    }

    private V tryGet(Object key) throws SQLException, IOException, ClassNotFoundException
    {
        long t1 = System.currentTimeMillis();
        V ret = null;
        byte[] data = null;
        Connection conn = null;
        try
        {
          conn = DB.openConnection("jelectrum_db");
          PreparedStatement ps = conn.prepareStatement("select * from "+table_name+" where key=?");
          ps.setString(1, key.toString());
          ResultSet rs = ps.executeQuery();

          if (rs.next())
          {
            data = rs.getBytes("data");
          }

          rs.close();
          ps.close();

        }
        finally
        {
          DB.safeClose(conn);
        }
        long t2 = System.currentTimeMillis();

        if (data != null)
        {
          ret = (V) (new ObjectInputStream(new ByteArrayInputStream(data)).readObject());
        } 

        return ret;
 

    }
    public int hashCode() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public boolean isEmpty()
    {
        return (size()==0);
    }

    public int size()
    {
      while(true)
      {
        try
        {
          return trySize();
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }

      }
 
    }
    private int trySize() throws SQLException
    {
        long t1 = System.currentTimeMillis();
        int count = 0;
        Connection conn = null;
        try
        {
          conn = DB.openConnection("jelectrum_db");
          PreparedStatement ps = conn.prepareStatement("select count(key) as count from "+table_name);
          ResultSet rs = ps.executeQuery();
          rs.next();

          count = rs.getInt("count");

          rs.close();
          ps.close();

        }
        finally
        {
          DB.safeClose(conn);
        }


        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);
        return count;

    }


    public Set<K> keySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V put(K key, V value)
    {
      while(true)
      {
        try
        {
          return tryPut(key,value,null);
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }
        catch(IOException e)
        {
          throw new RuntimeException(e);
        }

      }
 

    }
    

    public V tryPut(K key, V value, Connection conn) throws SQLException, IOException
    {
        long t1 = System.currentTimeMillis();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(bout);
        oout.writeObject(value);
        oout.flush();
        byte[] data = bout.toByteArray();
        oout.close();
        boolean local_conn_manage=false;

        try
        {
          if (conn == null)
          {
            local_conn_manage=true;
            conn = DB.openConnection("jelectrum_db");
          }

          boolean insert=true;

          {
          PreparedStatement ps = conn.prepareStatement("select count(key) as count from "+table_name+" where key=?");
          ps.setString(1, key.toString());
          ResultSet rs = ps.executeQuery();
          rs.next();

          int count = rs.getInt("count");
          if (count > 0) insert=false;
          }


          if (insert)
          {
            PreparedStatement ps = conn.prepareStatement("insert into "+table_name+" (key,data) values (?,?)");
            ps.setString(1, key.toString());
            ps.setBytes(2, data);
            ps.execute();
            ps.close();
          }
          else
          {

              PreparedStatement ps = conn.prepareStatement("update "+table_name+" set data=? where key = ?");
              ps.setBytes(1, data);
              ps.setString(2, key.toString());
              ps.execute();
              ps.close();
          }
        }
        finally
        {
          if (local_conn_manage)
          {
            DB.safeClose(conn);
          }
        }

        long t2 = System.currentTimeMillis();
        put_stats.addDataPoint(t2-t1);
        return null;
    }
    public void putAll(Map<? extends K,? extends V> m) 
    {
      while(true)
      {
        try
        {
          tryPutAll(m);
          return;
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }
        catch(IOException e)
        {
          throw new RuntimeException(e);
        }

      }

  
    }
    private void tryPutAll(Map<? extends K,? extends V> m) throws SQLException, IOException
    {
      Connection conn = null;
      try
      {
        conn = DB.openConnection("jelectrum_db");
        conn.setAutoCommit(false);

        for(Map.Entry e : m.entrySet())
        {
          tryPut((K)e.getKey(), (V)e.getValue(), conn);
        }
        conn.commit();
        
      }
      finally
      {
        DB.safeClose(conn);
      }

    }

    public V remove(Object key) 
    {
        throw new RuntimeException("not implemented - is stupid");

    }

    public Collection<V>   values() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public static void printStats()
    {
        DecimalFormat df = new DecimalFormat("0.0");

        get_stats.copyAndReset().print("get", df);
        put_stats.copyAndReset().print("put", df);

    }



}

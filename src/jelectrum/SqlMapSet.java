package jelectrum;

import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.LinkedList;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;

import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.Random;
import java.text.DecimalFormat;

import com.google.bitcoin.core.Sha256Hash;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;

import java.sql.SQLException;
import java.io.IOException;


public class SqlMapSet<K> implements MapSet<K, Sha256Hash>
{

    private static StatData put_stats = new StatData();
    private static StatData get_stats = new StatData();
    private String table_name;

    public SqlMapSet(String table_name, int max_key_len)
    { 
      this.table_name = table_name;
      createTable(max_key_len);
    }

    public void createTable(int max_key_len)
    { 
      Connection conn=null;
      try
      { 
        conn = DB.openConnection("jelectrum_db");

        {
          PreparedStatement ps = conn.prepareStatement("create table "+table_name+" ( node char("+max_key_len+") NOT NULL, element char(64))");
          ps.execute();
          ps.close();
        }
        {
          PreparedStatement ps = conn.prepareStatement("create unique index "+table_name+"_idx on "+table_name+"(node,element);");
          ps.execute();
          ps.close();
        }

      }
      catch(SQLException e)
      {
      }
      finally
      { 
        DB.safeClose(conn);
      }

    }


    public Set<Sha256Hash> getSet(Object key)
    {
      while(true)
      {
        try
        {
          return tryGetSet(key);
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }

      }
    }



    public Set<Sha256Hash> tryGetSet(Object key) throws SQLException
    {
        long t1 = System.currentTimeMillis();
        HashSet<Sha256Hash> set = new HashSet<Sha256Hash>();
        Connection conn=null;
        try
        { 
          conn = DB.openConnection("jelectrum_db");
          PreparedStatement ps = conn.prepareStatement("select * from "+table_name+" where node=?");
          ps.setString(1, key.toString());
          ResultSet rs = ps.executeQuery();

          
          while(rs.next())
          {
            set.add(new Sha256Hash(rs.getString("element")));
 
          }
          
          rs.close();
          ps.close();

        }
        finally
        { 
          DB.safeClose(conn);
        }

        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);
        return set;



    }

    public void add(K key, Sha256Hash value)
    {
      while(true)
      {
        try
        {
          tryAdd(key, value, null);
          return;
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
          return;
        }

      }

    }    

    public void tryAdd(K key, Sha256Hash value, Connection conn)     
      throws SQLException
    {
        long t1 = System.currentTimeMillis();
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
            PreparedStatement ps = conn.prepareStatement("select count(node) as count from "+table_name+" where node=? and element=?");
            ps.setString(1, key.toString());
            ps.setString(2, value.toString());
            ResultSet rs = ps.executeQuery();
            rs.next();

            int count = rs.getInt("count");
            if (count > 0) insert=false;
          }


          if (insert)
          {
            PreparedStatement ps = conn.prepareStatement("insert into "+table_name+" (node,element) values (?,?)");
            ps.setString(1, key.toString());
            ps.setString(2, value.toString());
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
    }

    public void addAll(Collection<K> keys, Sha256Hash val)
    {
      LinkedList<Map.Entry<K, Sha256Hash> > lst = new LinkedList<Map.Entry<K, Sha256Hash> >();
      for(K key : keys)
      {
        lst.add(new java.util.AbstractMap.SimpleEntry<K,Sha256Hash>(key, val));
      }
      addAll(lst);
    }

    public void addAll(Collection<Map.Entry<K,Sha256Hash>> lst)
    {
      while(true)
      {
        try
        {
          tryAddAll(lst);
          return;
        }
        catch(SQLException e)
        {
          e.printStackTrace();
          try{Thread.sleep(1000);}catch(Exception e2){}
        }

      }


    }

    private void tryAddAll(Collection<Map.Entry<K,Sha256Hash>> lst)
      throws SQLException
    {
      Connection conn = null;
      try
      {
        conn = DB.openConnection("jelectrum_db");
        conn.setAutoCommit(false);

        for(Map.Entry e : lst)
        {
          tryAdd((K)e.getKey(), (Sha256Hash)e.getValue(), conn);
        }
        conn.commit();

      }
      finally
      {
        DB.safeClose(conn);
      }


    }




    public static void printStats()
    {
        DecimalFormat df = new DecimalFormat("0.0");

        get_stats.copyAndReset().print("get", df);
        put_stats.copyAndReset().print("put", df);

    }



}

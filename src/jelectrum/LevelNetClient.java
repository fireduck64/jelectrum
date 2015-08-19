package jelectrum;


import java.net.Socket;
import java.nio.ByteBuffer;

import java.io.DataInputStream;

import java.util.Random;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;

import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.LinkedBlockingQueue;


public class LevelNetClient
{
  public static final int RESULT_GOOD = 1273252631;
  public static final int RESULT_BAD = 9999;
  public static final int RESULT_NOTFOUND = 133133;
  public static final int SOCKET_TIMEOUT = 15000;

  private Jelectrum jelly;

  private Config config;

  private String host;
  private int port;

  private LinkedBlockingQueue<LevelConnection> conns;

  private Semaphore open_sem;

  public boolean throw_on_error=false;

  public LevelNetClient(Jelectrum jelly, Config config) throws Exception
  {
    this.jelly = jelly;
    this.config = config;

    config.require("leveldb_host");
    config.require("leveldb_port");
    config.require("leveldb_conns");


    this.host = config.get("leveldb_host");
    this.port = config.getInt("leveldb_port");

    open_sem = new Semaphore(config.getInt("leveldb_conns"));

    conns = new LinkedBlockingQueue<LevelConnection>();

  }

  private LevelConnection getConnection()
  {
    LevelConnection conn = conns.poll();
    if (conn != null) return conn;

    if (open_sem.tryAcquire())
    {
      while(true)
      {
        try
        {
          conn = new LevelConnection();
          return conn;
        }
        catch(java.io.IOException e)
        {
          jelly.getEventLog().alarm("LevelDB connection failure: " + e);
          if (throw_on_error) throw new RuntimeException(e);
          try{ Thread.sleep(2500); } catch(Throwable t){}
        }
      }
    }

    try
    {
      return conns.take();
    }
    catch(InterruptedException e)
    {
      throw new RuntimeException(e);
    }
  }

  private void returnConnection(LevelConnection conn)
  {
    if (conn == null) return;
    try
    {
      conns.put(conn);
    }
    catch(InterruptedException e)
    {
      throw new RuntimeException(e);
    }

  }

  private void trashConnection(LevelConnection conn)
  {
    if (conn != null)
    {
      open_sem.release(1);
      conn.close();
    }
  }


  public ByteBuffer get(String key)
  {
    while(true)
    {
      LevelConnection conn = null;
      try
      {
        conn = getConnection();
        return conn.get(key);

      }
      catch(java.io.IOException e)
      {
        trashConnection(conn); conn=null;

        jelly.getEventLog().log("LevelDB error: " + e);
        if (throw_on_error) throw new RuntimeException(e);
        try{ Thread.sleep(2500); } catch(Throwable t){}
      }
      finally
      {
        returnConnection(conn);
      }
    }
  
  }

  public void put(String key, ByteBuffer value)
  {
    while(true)
    {
      LevelConnection conn = null;
      try
      {
        conn = getConnection();
        conn.put(key, value);
        return;

      }
      catch(java.io.IOException e)
      {
        trashConnection(conn); conn=null;
        jelly.getEventLog().log("LevelDB error: " + e);
        if (throw_on_error) throw new RuntimeException(e);
        try{ Thread.sleep(2500); } catch(Throwable t){}
      }
      finally
      {
        returnConnection(conn);
      }
    }
  
  }

  public void putAll(Map<String, ByteBuffer> m)
  {
    while(true)
    {
      LevelConnection conn = null;
      try
      {
        conn = getConnection();
        conn.putAll(m);
        return;

      }
      catch(java.io.IOException e)
      {
        trashConnection(conn); conn=null;
        jelly.getEventLog().log("LevelDB error: " + e);
        if (throw_on_error) throw new RuntimeException(e);
        try{ Thread.sleep(2500); } catch(Throwable t){}
      }
      finally
      {
        returnConnection(conn);
      }
    }
 
  
  }

  public Map<String, ByteBuffer> getByPrefix(String prefix)
  {
    while(true)
    {
      LevelConnection conn = null;
      try
      {
        conn = getConnection();
        return conn.getByPrefix(prefix);
      }
      catch(java.io.IOException e)
      {
        trashConnection(conn); conn=null;
        jelly.getEventLog().log("LevelDB error: " + e);
        if (throw_on_error) throw new RuntimeException(e);
        try{ Thread.sleep(2500); } catch(Throwable t){}
      }
      finally
      {
        returnConnection(conn);
      }
    }
 
  }


  public class LevelConnection
  {
    public LevelConnection()
      throws java.io.IOException
    {

      try
      {
        sock = new Socket(host, port);
        sock.setTcpNoDelay(true);
        sock.setSoTimeout(SOCKET_TIMEOUT);


        d_in = new DataInputStream(sock.getInputStream());
      }
      catch(java.lang.SecurityException e)
      {
        throw new RuntimeException(e);
      }
    }


    private Socket sock;
    private DataInputStream d_in;

    public void close()
    {
      try
      {
        sock.close();
      }
      catch(Throwable t){}
    }


    public ByteBuffer get(String key)
      throws java.io.IOException
    {
      byte cmd[]=new byte[2];
      cmd[0]=1;
      cmd[1]=0;

      sock.getOutputStream().write(cmd, 0, 2);

      writeString(key);
      sock.getOutputStream().flush();
      int status = readInt();

      if (status == RESULT_NOTFOUND) return null;
      else if (status != RESULT_GOOD) throw new java.io.IOException("Bad result: " + status);

      return readBytes();
    }


    public void put(String key, ByteBuffer value)
      throws java.io.IOException
    {
      byte cmd[]=new byte[2];
      cmd[0]=2;
      cmd[1]=0;

      sock.getOutputStream().write(cmd, 0, 2);

      writeString(key);
      writeByteArray(value);

      sock.getOutputStream().flush();

      int status = readInt();
      if (status != RESULT_GOOD) throw new java.io.IOException("Bad result: " + status);
    }

    public void putAll(Map<String, ByteBuffer> map)
      throws java.io.IOException
    {
      byte cmd[]=new byte[2];
      cmd[0]=3;
      cmd[1]=0;

      sock.getOutputStream().write(cmd, 0, 2);

      writeInt(map.size());

      for(Map.Entry<String, ByteBuffer> me : map.entrySet())
      {
        writeString(me.getKey());
        writeByteArray(me.getValue());
      }
      int status = readInt();
      if (status != RESULT_GOOD) throw new java.io.IOException("Bad result: " + status);

    }

    public Map<String, ByteBuffer> getByPrefix(String prefix)
      throws java.io.IOException
    {
      byte cmd[]=new byte[2];
      cmd[0]=4;
      cmd[1]=0;

      sock.getOutputStream().write(cmd, 0, 2);

      writeString(prefix);

      int status = readInt();
      int items = readInt();

      Map<String,ByteBuffer> m = new TreeMap<String,ByteBuffer>();
      for(int i=0; i<items; i++)
      {
        String key = readString();
        ByteBuffer buff = readBytes();
        m.put(key, buff);
      }

      return m;

    }

    private ByteBuffer readBytes()
      throws java.io.IOException
    {
      int sz = readInt();

      if (sz == 0) return null;

      byte[] data = new byte[sz];
      d_in.readFully(data);

      return ByteBuffer.wrap(data);
    }



    private String readString()
      throws java.io.IOException
    {
      return new String(readBytes().array());

    }

    private int readInt()
      throws java.io.IOException
    {
      byte[] d = new byte[4];
      d_in.readFully(d);

      ByteBuffer bb = ByteBuffer.wrap(d);
      bb.order(java.nio.ByteOrder.BIG_ENDIAN);

      return bb.getInt();
    }

    private void writeNull()
      throws java.io.IOException
    {
        writeInt(0);

    }

    private void writeInt(int val)
      throws java.io.IOException
    {
      byte[] val_bytes=new byte[4];

      ByteBuffer bb = ByteBuffer.wrap(val_bytes);
      bb.order(java.nio.ByteOrder.BIG_ENDIAN);
      bb.putInt(val);

      sock.getOutputStream().write(val_bytes);
    }

    private void writeString(String s)
      throws java.io.IOException
    {
      if (s == null)
      {
        writeNull();
        return;
      }
      byte[] bytes = s.getBytes();
      writeInt(bytes.length);
      sock.getOutputStream().write(bytes);
    }

    private void writeByteArray(ByteBuffer bb)
      throws java.io.IOException
    {
      if (bb == null)
      {
        writeNull();
        return;
      }
      byte[] bytes = bb.array();
      writeInt(bytes.length);
      sock.getOutputStream().write(bytes);
    }

  }

}

package jelectrum;

import org.jibble.pircbot.PircBot;
import org.jibble.pircbot.IrcException;


public class IrcBot extends PircBot
{
  private Jelectrum jelly;
  private Object connection_lock = new Object();
  private Config config;

  private long kickWaitTime=0;

  public IrcBot(Jelectrum jelly)
  {
    config = jelly.getConfig();


    if (config.isSet("irc_enabled") && config.getBoolean("irc_enabled"))
    {
      config.require("irc_nick");
      config.require("irc_advertise_host");
      setName("E_j_" + config.get("irc_nick"));
      setLogin("E_j_" + config.get("irc_nick"));

    }

    this.jelly = jelly;

  }
  public String getAdvertString()
  {
    StringBuilder sb=new StringBuilder();
    sb.append(config.get("irc_advertise_host"));
    sb.append(" v");
    sb.append(StratumConnection.PROTO_VERSION);
    sb.append(" p10000");
    if (config.isSet("tcp_port")) sb.append(" t");
    if (config.isSet("ssl_port")) sb.append(" s");

    return sb.toString();


  }


  public void start()
  {
    if (config.getBoolean("irc_enabled"))
    {
      new IrcThread().start();
    }
  }

  @Override
  protected void onDisconnect() 
  {
    synchronized(connection_lock)
    {
      connection_lock.notifyAll();
    }
  }

  @Override
  protected void onKick(String channel, String kickerNick, String kickerLogin, String kickerHostname, String recipientNick, String reason)
  {
    jelly.getEventLog().log("Kicked from " + channel + " by " + kickerNick + " for " + reason);
    jelly.getEventLog().log("Waiting one hour before trying again");


    kickWaitTime = System.currentTimeMillis() + 3600L * 1000L;

    synchronized(connection_lock)
    {
      connection_lock.notifyAll();
    }
  }


  public class IrcThread extends Thread
  {

    public IrcThread()
    {
      setName("IrcThread");
      setDaemon(true);


    }

    public void run()
    {
      while(true)
      {
        try
        {
          ircRun();

        }
        catch(Throwable t)
        {
          t.printStackTrace();
        }

        try { sleep(600000);} catch(Throwable t){}

      }

    }

    public void ircRun() throws java.io.IOException, IrcException, InterruptedException
    {
      //setVerbose(true);
      
      setVersion(getAdvertString());
      connect("irc.freenode.net");


      joinChannel("#electrum");
      joinChannel("#jelectrum");

      synchronized(connection_lock)
      {
        connection_lock.wait();
      }

      if (System.currentTimeMillis() < kickWaitTime)
      {
        Thread.sleep(15000);
      }


      disconnect();

    }
  }


}

package jelectrum;

import org.jibble.pircbot.PircBot;
import org.jibble.pircbot.IrcException;


public class IrcBot extends PircBot
{
  private Jelectrum jelly;
  private Object connection_lock = new Object();
  private Config config;

  private volatile long kickWaitTime=0;
  private boolean shouldRun = false;
  private String advert_host = null;
  private String nick = null;

  public IrcBot(Jelectrum jelly, String mode)
  {
    config = jelly.getConfig();

    if (mode == null)
    {

      if (config.isSet("irc_enabled") && config.getBoolean("irc_enabled"))
      {
        config.require("irc_nick");
        config.require("irc_advertise_host");
        nick = ("E_j_" + config.get("irc_nick"));
        advert_host = config.get("irc_advertise_host");

        setName(nick);
        setLogin(nick);
        shouldRun=true;

      }
    }
    else 
    {
      if (config.isSet("irc_enabled_" + mode) && config.getBoolean("irc_enabled_" + mode))
      {
        config.require("irc_nick_"+mode);
        config.require("irc_advertise_host_"+mode);

        nick = "E_j_" + config.get("irc_nick_"+mode);
        advert_host = config.get("irc_advertise_host_"+mode);

        setName(nick);
        setLogin(nick);
        shouldRun=true;

      }
 
    }

    this.jelly = jelly;

  }
  public String getAdvertString()
  {
    //hostmame v1.0 p10000 t s
    //hostname v1.0 p10000 t50003 s50004

    StringBuilder sb=new StringBuilder();
    sb.append(advert_host);
    sb.append(" v");
    sb.append(StratumConnection.PROTO_VERSION);
    sb.append(" p10000");
    if (config.isSet("tcp_port")) 
    {
      sb.append(" t");
      int port = jelly.getStratumServer().getTcpPort();
      if (port != 50001) sb.append("" + port);
    }
    if (config.isSet("ssl_port"))
    {
      sb.append(" s");
      int port = jelly.getStratumServer().getSslPort();
      if (port != 50002) sb.append("" + port);
    }

    return sb.toString();


  }


  public void start()
  {
    if (shouldRun)
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
    if (nick.equals(recipientNick))
    {
      jelly.getEventLog().log("Kicked from " + channel + " by " + kickerNick + " for " + reason);
      jelly.getEventLog().log("Waiting one hour before trying again");

      kickWaitTime = System.currentTimeMillis() + 3600L * 1000L;

      synchronized(connection_lock)
      {
        connection_lock.notifyAll();
      }
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
      while(shouldRun)
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
      
      while (System.currentTimeMillis() < kickWaitTime)
      {
        Thread.sleep(15000);
      }

      setVersion(getAdvertString());
      connect("irc.freenode.net");

      joinChannel("#electrum");
      joinChannel("#jelectrum");

      synchronized(connection_lock)
      {
        connection_lock.wait();
      }

      while (System.currentTimeMillis() < kickWaitTime)
      {
        Thread.sleep(15000);
      }

      disconnect();

    }
  }


}

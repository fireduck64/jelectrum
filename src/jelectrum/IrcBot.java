package jelectrum;

import org.jibble.pircbot.PircBot;
import org.jibble.pircbot.IrcException;

import java.util.Scanner;
import java.net.URL;
import java.util.Random;

public class IrcBot extends PircBot
{
  private Jelectrum jelly;
  private Object connection_lock = new Object();
  private Config config;

  private volatile long kickWaitTime=0;
  private boolean shouldRun = false;
  private String advert_host = null;
  private String nick = null;

  private String chatChannel;
  private boolean gossip=false;

  public IrcBot(Jelectrum jelly, String mode)
  {
    config = jelly.getConfig();
    this.jelly = jelly;

    if (mode == null)
    {

      if (config.isSet("irc_enabled") && config.getBoolean("irc_enabled"))
      {
        config.require("irc_nick");
        config.require("irc_advertise_host");
        advert_host = config.get("irc_advertise_host");

        String config_nick = config.get("irc_nick");

        if (config_nick.equals("auto"))
        {
          String seed = getHostSeed(advert_host);
          config_nick = WordList.getWords(2, "_", seed);
        }

        nick = "E_j_" + config_nick;

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

        advert_host = config.get("irc_advertise_host_"+mode);

        String config_nick = config.get("irc_nick_"+mode);

        if (config_nick.equals("auto"))
        {
          String seed = getHostSeed(advert_host);
          config_nick = WordList.getWords(2, "_", seed);
        }

        nick = "E_j_" + config_nick;

        setName(nick);
        setLogin(nick);
        shouldRun=true;

      }
 
    }
    new GossipThread().start();

  }

  public String getHostSeed(String h)
  { 
    try
    {
      if (h.equals("auto"))
      {
        h = lookupMyIp();
      }
    }
    catch(java.io.IOException e)
    {
      return null;
    }
    return h;


  }
  public String getAdvertString()
    throws java.io.IOException
  {
    //hostmame v1.0 p10000 t s
    //hostname v1.0 p10000 t50003 s50004
    String host = advert_host;

    if (advert_host.equals("auto"))
    {
      host = lookupMyIp();
    }

    StringBuilder sb=new StringBuilder();
    sb.append(host);
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
        gossip=false;

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

      if (config.getBoolean("testnet"))
      {
        joinChannel("#electrum-testnet");
        joinChannel("#jelectrum-testnet");
        chatChannel="#jelectrum-testnet";
      }
      else
      {
        joinChannel("#electrum");
        joinChannel("#jelectrum");
        chatChannel="#jelectrum";
      }
      gossip=true;

      synchronized(connection_lock)
      {
        connection_lock.wait();
      }

      gossip=false;

      while (System.currentTimeMillis() < kickWaitTime)
      {
        Thread.sleep(15000);
      }

      disconnect();

    }
  }


  public static String lookupMyIp()
    throws java.io.IOException
  {
    String url = "https://jelectrum-1022.appspot.com/myip";

    URL u = new URL(url);
    Scanner scan =new Scanner(u.openStream());
    String line = scan.nextLine();
    scan.close();

    return line.trim();

  }

  public class GossipThread extends Thread
  {
    public GossipThread()
    {
      setName("IrcBot/Gossip");
      setDaemon(false);

    }

    public void run()
    {
      Random rnd = new Random();
      while(true)
      {
        int sleep_max = 3 * 3600 * 1000;
        try
        {
          sleep(rnd.nextInt(sleep_max));
          if (gossip)
          {
            int word_count = 1 + rnd.nextInt(5);
            sendMessage(chatChannel, WordList.getWords(word_count, " ", null));
          }
        }
        catch(Throwable t)
        {
          t.printStackTrace();
        }

      }

    }

  }

}

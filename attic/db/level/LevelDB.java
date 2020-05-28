package jelectrum.db.level;

import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.Config;
import jelectrum.EventLog;


public class LevelDB extends jelectrum.db.DB
{
  protected LevelNetClient client;

  public LevelDB(EventLog log, Config config)
    throws Exception
  {
    super(config);

    client = new LevelNetClient(log, config);

    open();
  }
  
  protected DBMap openMap(String name) throws Exception
  {
    return new LevelDBMap(client, name);

    
  }
  protected DBMapSet openMapSet(String name) throws Exception
  {
    return new LevelDBMapSet(client, name);

  }


  

  



  

}

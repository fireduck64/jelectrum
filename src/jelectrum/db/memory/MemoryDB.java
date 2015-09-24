package jelectrum.db.memory;
import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;


import jelectrum.Config;

public class MemoryDB extends DB
{
  public MemoryDB(Config conf)
    throws Exception
  {
    super(conf);

    open();
  }

  @Override
  protected DBMapSet openMapSet(String name)
  {
    return new MemoryMapSet();
  }

  @Override
  protected DBMap openMap(String name)
  {
    return new MemoryMap(); 
  }



}


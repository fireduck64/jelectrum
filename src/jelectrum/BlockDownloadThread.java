
package jelectrum;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;

public class BlockDownloadThread extends Thread
{
  private Jelectrum jelly;

  public BlockDownloadThread(Jelectrum jelly)
  {
    this.jelly = jelly;
    setName("BlockDownloadThread");
    setDaemon(true);
  }

  public void run()
  {
    while(true)
    {
      try
      {
        runInternal();
      }
      catch(Throwable t)
      {
        jelly.getEventLog().alarm("BlockDownloadThread Error - " + t);
        t.printStackTrace();
      }
      try
      {
        Thread.sleep(5000);
      }
      catch(Throwable t)
      {
        t.printStackTrace();
      }

    }
  }

  private void runInternal() throws Exception
  {
    int bitcoind_height = jelly.getBitcoinRPC().getBlockHeight();
    int local_height = getStartingHeight();

    jelly.getEventLog().log(String.format("Bitcoind: %d Local %d", bitcoind_height, local_height));


    for(int i=local_height+1; i<=bitcoind_height; i++)
    {
      downloadBlock(i);
    }

  }

  private int getStartingHeight() throws Exception
  {
    int local_height = jelly.getElectrumNotifier().getHeadHeight();

    int reorg_size=0;
    while(true)
    {
      if (local_height == 0) return local_height;

      Sha256Hash hash = jelly.getBitcoinRPC().getBlockHash(local_height);
      if (jelly.getDB().getBlockSavedMap().containsKey(hash))
      { 
        if (reorg_size > 0) jelly.getEventLog().alarm("Reorg of size: " + reorg_size);
        return local_height;
      }

      // hash not saved, stepping back
      local_height--;
      reorg_size++;
    }

  }

  private void downloadBlock(int height)
    throws Exception
  {
    Sha256Hash hash = jelly.getBitcoinRPC().getBlockHash(height);

    SerializedBlock block = jelly.getBitcoinRPC().getBlock(hash);
    Block b = block.getBlock(jelly.getNetworkParameters());

    jelly.getBlockStore().put(b);


    jelly.getImporter().saveBlock(b);
  }
  

}

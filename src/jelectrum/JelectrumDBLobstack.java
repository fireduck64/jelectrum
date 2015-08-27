package jelectrum;

import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedList;
import java.io.File;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;

import jelectrum.LobstackMap.ConversionMode;
import java.io.FileOutputStream;
import java.io.PrintStream;

import lobstack.Lobstack;

public class JelectrumDBLobstack extends JelectrumDB
{
  private Jelectrum jelly;
    private Config conf;
    protected LobstackMapSet address_to_tx_map;
    protected LobstackMapSet tx_to_block_map;
    protected boolean compress;

    protected LinkedList<Lobstack> stack_list;
    protected PrintStream cleanup_log;

    public JelectrumDBLobstack(Jelectrum jelly, Config config)
        throws Exception
    {
        super(config);
        this.conf = config;
        this.jelly = jelly;
        compress=false;

        stack_list = new LinkedList<Lobstack>();

        config.require("lobstack_path");
        config.require("lobstack_minfree_gb");
        if (config.isSet("lobstack_compress")) compress = config.getBoolean("lobstack_compress");

        cleanup_log = new PrintStream(new FileOutputStream("lobstack.log", true));

        open();
    }

    public synchronized void open()
    {

        try
        {
          File path = new File(conf.get("lobstack_path"));

          path.mkdirs();
          long min_space = conf.getInt("lobstack_minfree_gb") * 1024L * 1024L * 1024L;

          new FreeSpaceWatcher(path, jelly, min_space).start();


            /*jelly.getEventLog().alarm("Doing compress");
            openStack("special_object_map").compress();
            openStack("special_block_store_map").compress();
            jelly.getEventLog().alarm("Compress done");*/

            Lobstack special_object_map_stack = openStack("special_object_map");

            Lobstack special_block_store_map_stack = openStack("special_block_store_map");



            tx_map = new LobstackMap<Sha256Hash, SerializedTransaction>(openStack("tx_map"), ConversionMode.SERIALIZEDTRANSACTION);
            block_store_map = new CacheMap<Sha256Hash, StoredBlock>(25000,new LobstackMap<Sha256Hash, StoredBlock>(openStack("block_store_map"),ConversionMode.OBJECT));
            special_block_store_map = new LobstackMap<String, StoredBlock>(special_block_store_map_stack,ConversionMode.OBJECT);
            block_map = new CacheMap<Sha256Hash, SerializedBlock>(240,new LobstackMap<Sha256Hash, SerializedBlock>(openStack("block_map"),ConversionMode.OBJECT));
            block_rescan_map = new LobstackMap<Sha256Hash, String>(openStack("block_rescan_map"),ConversionMode.STRING);
            special_object_map = new LobstackMap<String, Object>(special_object_map_stack,ConversionMode.OBJECT);
            header_chunk_map = new CacheMap<Integer, String>(200, new LobstackMap<Integer, String>(openStack("header_chunk_map"),ConversionMode.STRING));
            utxo_trie_map = new CacheMap<String, UtxoTrieNode>(750000, new LobstackMap<String, UtxoTrieNode>(openStack("utxo_trie_map"),ConversionMode.OBJECT));

            address_to_tx_map = new LobstackMapSet(openStack("address_to_tx_map", 1));
            tx_to_block_map = new LobstackMapSet(openStack("tx_to_block_map"));


            new LobstackMaintThread().start();

        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }


    }
    private Lobstack openStack(String name)
      throws java.io.IOException
    {
      return openStack(name, 2);
    }
 
    private Lobstack openStack(String name, int key_step_size)
      throws java.io.IOException
    {
      Lobstack l = new Lobstack(new File(conf.get("lobstack_path")), name, compress, key_step_size);

      synchronized(stack_list)
      {
        stack_list.add(l);
      }
      return l;
    }


    public synchronized Map<Sha256Hash, StoredBlock> getBlockStoreMap()
    {   
        return block_store_map;
    }

    public synchronized Map<String, StoredBlock> getSpecialBlockStoreMap()
    {   
        return special_block_store_map;
    }

    public synchronized Map<Sha256Hash,SerializedTransaction> getTransactionMap()
    {   
        return tx_map;
    }
    public synchronized Map<Sha256Hash, SerializedBlock> getBlockMap()
    {   
        return block_map;
    }


    public void addAddressToTxMap(String address, Sha256Hash hash)
    {
      address_to_tx_map.put(address, hash);
    }
    @Override
    public void addAddressesToTxMap(Collection<String> addresses, Sha256Hash hash)
    {
      LinkedList<Map.Entry<String, Sha256Hash> > lst = new LinkedList<Map.Entry<String, Sha256Hash> >();
      for(String a : addresses)
      {
        lst.add(new java.util.AbstractMap.SimpleEntry<String,Sha256Hash>(a, hash));
      }
      addAddressesToTxMap(lst);
      
    }
    @Override
    public void addAddressesToTxMap(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      address_to_tx_map.putList(lst);
    }
    public Set<Sha256Hash> getAddressToTxSet(String address)
    {
        return address_to_tx_map.getSet(address);
    }

   
  
    public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block)
    {
      tx_to_block_map.put(tx.toString(), block);
    }
    @Override
    public void addTxsToBlockMap(Collection<Sha256Hash> txs, Sha256Hash block)
    {

      LinkedList<Map.Entry<String, Sha256Hash> > lst = new LinkedList<Map.Entry<String, Sha256Hash> >();
      for(Sha256Hash a : txs)
      {
        lst.add(new java.util.AbstractMap.SimpleEntry<String,Sha256Hash>(a.toString(), block));
      }
 
      tx_to_block_map.putList(lst);

    }
    public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx)
    {
        return tx_to_block_map.getSet(tx.toString());
    }

    public synchronized Map<Sha256Hash, String> getBlockRescanMap()
    {
        return block_rescan_map;
    }

    public synchronized Map<String, Object> getSpecialObjectMap()
    {
        return special_object_map;
    }

    public synchronized Map<Integer, String> getHeaderChunkMap()
    {
        return header_chunk_map;
    }


    public class LobstackMaintThread extends Thread
    {
      public LobstackMaintThread()
      {
        setName("LobstackMaintThread");
        setDaemon(true);
      }

      public void run()
      {
        while(true)
        {
          try
          {
            boolean done_something = false;

            for(Lobstack ls : stack_list)
            {
              //ls.printTimeReport(cleanup_log);
              if (ls.cleanup(0.75, 2L * 1024L * 1024L * 1024L, cleanup_log))
              {
                done_something=true;
              }
            }
            if (!done_something)
            {
              cleanup_log.println("Sleeping");
              sleep(300L * 1000L);
            }
          }
          catch(Exception e)
          {
            e.printStackTrace();
          }
        }
      }
  }
}

          

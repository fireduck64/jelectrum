package jelectrum;

import com.google.bitcoin.store.H2FullPrunedBlockStore;
import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.core.BlockChain;
import com.google.bitcoin.core.PeerGroup;
import com.google.bitcoin.core.PeerAddress;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.Sha256Hash;
import org.apache.commons.codec.binary.Hex;
import java.net.InetAddress;
import com.google.bitcoin.net.discovery.DnsDiscovery;
import com.google.bitcoin.net.discovery.IrcDiscovery;
import com.google.bitcoin.core.DownloadListener;

import java.util.LinkedList;
import jelectrum.db.DBFace;

public class Jelectrum
{
    public static void main(String args[]) throws Exception
    {
        new Jelectrum(new Config(args[0])).start();

    }

    private Config config;
    private DBFace jelectrum_db;
    private Importer importer;

    private MapBlockStore block_store;
    private BlockChain block_chain;
    private BlockChainCache block_chain_cache; 
    private PeerGroup peer_group;
    private EventLog event_log;

    private StratumServer stratum_server;
    private NetworkParameters network_params;
    private ElectrumNotifier notifier;
    private HeaderChunkAgent header_chunk_agent;
    private BitcoinRPC bitcoin_rpc;
    private UtxoTrieMgr utxo_trie_mgr;

    private volatile boolean caught_up=false;

    public Jelectrum(Config conf)
        throws Exception
    {
        config = conf;
        network_params = Util.getNetworkParameters(config);

        config.require("bitcoin_network_use_peers");
        config.require("db_type");
        config.require("bitcoin_peer_host");
        config.require("bitcoin_peer_port");

        event_log = new EventLog(config);

        String db_type = config.get("db_type");

        //jelectrum_db = new JelectrumDBMapDB(config);
        //jelectrum_db = new JelectrumDBDirect(config);
        //jelectrum_db = new JelectrumDBCloudData(config);

        bitcoin_rpc = new BitcoinRPC(config);
        bitcoin_rpc.testConnection();

        if (db_type.equals("mongo"))
        {
          jelectrum_db = new jelectrum.db.mongo.MongoDB(config);
        }
        /*else if (db_type.equals("sql"))
        {
          jelectrum_db = new JelectrumDBSQL(config);
        }*/
        else if (db_type.equals("leveldb"))
        {
          jelectrum_db = new jelectrum.db.level.LevelDB(event_log, config);
        }
        else if (db_type.equals("lobstack"))
        {
          jelectrum_db = new jelectrum.db.lobstack.LobstackDB(this, config);
        }
        else if (db_type.equals("lmdb"))
        {
          jelectrum_db = new jelectrum.db.lmdb.LMDB(config);
        }
        else if (db_type.equals("little"))
        {
          jelectrum_db = new jelectrum.db.little.LittleDB(config, event_log, bitcoin_rpc);
        }
        else if (db_type.equals("memory"))
        {
          jelectrum_db = new jelectrum.db.memory.MemoryDB(config);
        }
        else
        {
          System.out.println("Unknown db_type: " + db_type);
          System.out.println("Try mongo or sql or leveldb or lobstack");
          System.exit(-1);
        }
        
        block_store = new MapBlockStore(this);
        
        block_chain = new BlockChain(network_params, block_store);
        
        utxo_trie_mgr = new UtxoTrieMgr(this);

        notifier = new ElectrumNotifier(this);
        
        importer = new Importer(network_params, this, block_store);

        stratum_server = new StratumServer(this, config);

        block_chain_cache = BlockChainCache.load(this);

        header_chunk_agent = new HeaderChunkAgent(this);




    }


    public void start()
        throws Exception
    {
        utxo_trie_mgr.getUtxoState();
        if (config.getBoolean("bulk_import_enabled"))
        {
          new BulkImporter(this);
        }

        System.out.println("Updating block chain cache");
        block_chain_cache.update(this, block_store.getChainHead());
        
        utxo_trie_mgr.start();


        System.out.println("Starting things");
        importer.start();
        notifier.start();

        stratum_server.setEventLog(event_log);

        stratum_server.start();


        System.out.println("Starting peer download");


        peer_group = new PeerGroup(network_params, block_chain);

        peer_group.setMaxConnections(16);
        peer_group.addAddress(new PeerAddress(InetAddress.getByName(config.get("bitcoin_peer_host")),config.getInt("bitcoin_peer_port")));

        if (config.isSet("bitcoin_peer_list"))
        {
          for(String peer : config.getList("bitcoin_peer_list"))
          {
            event_log.log("Adding additional peer: " + peer);
            peer_group.addAddress(new PeerAddress(InetAddress.getByName(peer),8333));
          
          }
        }

        if (config.getBoolean("bitcoin_network_use_peers"))
        {
            peer_group.addPeerDiscovery(new DnsDiscovery(network_params));
            peer_group.addPeerDiscovery(new IrcDiscovery("#bitcoin"));
        }
        peer_group.addEventListener(new DownloadListener());
        peer_group.addEventListener(new ImportEventListener(importer));
        peer_group.setMinBroadcastConnections(1);

        peer_group.start();
        peer_group.downloadBlockChain();
        //Thread.sleep(250);
        while (peer_group.numConnectedPeers() == 0)
        {
          event_log.alarm("No connected peers - can't get new blocks or transactions");
          Thread.sleep(5000);
        }


        while(bitcoin_rpc.getBlockHeight() > notifier.getHeadHeight())
        {
            Thread.sleep(5000);
        }

        System.out.println("Block chain caught up");
        event_log.log("Block chain caught up");
        caught_up=true;
        new IrcBot(this,null).start();
        new IrcBot(this,"onion").start();

        importer.setBlockPrintEvery(1);
        importer.disableRatePrinting();
        

        header_chunk_agent.start();

        if (config.getBoolean("block_repo_saver"))
        {
          new BlockRepoSaver(this,100).start();
          new BlockRepoSaver(this,10).start();
        }



        while(true)
        {
            if (bitcoin_rpc.getBlockHeight() > notifier.getHeadHeight()+3)
            {
                event_log.alarm("We are far behind.  Aborting.");
                System.exit(1);
            }

            Thread.sleep(25000);
        }
        /*peer_group.stop();
        jelectrum_db.commit();
        jelectrum_db.close();*/

    }

    public boolean isUpToDate()
    {
      return caught_up;
    }

    public Config getConfig()
    {
        return config;
    }

    public Importer getImporter()
    {
        return importer;
    }

    public DBFace getDB()
    {
        return jelectrum_db;
    }

    public NetworkParameters getNetworkParameters()
    {
        return network_params;
    }
    public ElectrumNotifier getElectrumNotifier()
    {
        return notifier;
    }

    public MapBlockStore getBlockStore()
    {
        return block_store;
    }

    public EventLog getEventLog()
    {
        return event_log;
    }
    public PeerGroup getPeerGroup()
    {
        return peer_group;
    }

    public BlockChainCache getBlockChainCache()
    {
        return block_chain_cache;
    }
    public HeaderChunkAgent getHeaderChunkAgent()
    {
        return header_chunk_agent;
    }

    public BitcoinRPC getBitcoinRPC()
    {
        return bitcoin_rpc;
    }

    public UtxoTrieMgr getUtxoTrieMgr()
    {
      return utxo_trie_mgr;
    }
    public StratumServer getStratumServer()
    {
      return stratum_server;
    }

    private volatile boolean space_limited;
    public void setSpaceLimited(boolean limited)
    {
      if (limited)
      {
        event_log.alarm("Going into space limited mode");
      }
      else
      {
        event_log.alarm("Returning from space limited mode");
      }
      space_limited = limited;
    }

    public boolean getSpaceLimited()
    {
      return space_limited;
    }
    

}

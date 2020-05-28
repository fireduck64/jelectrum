package jelectrum;

import org.bitcoinj.store.H2FullPrunedBlockStore;
import org.bitcoinj.store.BlockStore;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.BlockChain;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.VersionMessage;
import org.apache.commons.codec.binary.Hex;
import java.net.InetAddress;
import org.bitcoinj.net.discovery.DnsDiscovery;

import java.util.LinkedList;
import jelectrum.db.DBFace;
import jelectrum.db.RawBitcoinDataSource;

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
    private EventLog event_log;

    private StratumServer stratum_server;
    private NetworkParameters network_params;
    private ElectrumNotifier notifier;
    private HeaderChunkAgent header_chunk_agent;
    private BitcoinRPC bitcoin_rpc;
    private UtxoSource utxo_source;
    private PeerManager peer_manager;
    private MemPooler mem_pooler;

    private volatile boolean caught_up=false;

    public Jelectrum(Config conf)
        throws Exception
    {
        config = conf;
        network_params = Util.getNetworkParameters(config);

        config.require("db_type");
        //config.require("bitcoin_peer_host");
        //config.require("bitcoin_peer_port");

        event_log = new EventLog(config);

        String db_type = config.get("db_type");

        //jelectrum_db = new JelectrumDBMapDB(config);
        //jelectrum_db = new JelectrumDBDirect(config);
        //jelectrum_db = new JelectrumDBCloudData(config);

        if (config.getBoolean("bitcoind_enable"))
        {
          bitcoin_rpc = new BitcoinRPC(config, event_log);
          bitcoin_rpc.testConnection();
        }
        else
        {
          throw new RuntimeException("Running without bitcoind_enable no longer supported");
        }

        /*if (db_type.equals("mongo"))
        {
          jelectrum_db = new jelectrum.db.mongo.MongoDB(config);
        }*/
        /*else if (db_type.equals("sql"))
        {
          jelectrum_db = new JelectrumDBSQL(config);
        }*/
        /*else if (db_type.equals("leveldb"))
        {
          jelectrum_db = new jelectrum.db.level.LevelDB(event_log, config);
        }*/
        if (db_type.equals("slopbucket"))
        {
          jelectrum_db = new jelectrum.db.slopbucket.SlopbucketDB(config, event_log);
        }
        else if (db_type.equals("lobstack"))
        {
          jelectrum_db = new jelectrum.db.lobstack.LobstackDB(this, config);
        }
        else if (db_type.equals("lmdb"))
        {
          jelectrum_db = new jelectrum.db.lmdb.LMDB(config);
        }
        else if (db_type.equals("memory"))
        {
          jelectrum_db = new jelectrum.db.memory.MemoryDB(config);
        }
        else if (db_type.equals("redis"))
        {
          jelectrum_db = new jelectrum.db.jedis.JedisDB(config);
        }
        else if (db_type.equals("rocksdb"))
        {
          jelectrum_db = new jelectrum.db.rocksdb.JRocksDB(config, event_log);
        }
        /*else if (db_type.equals("cassandra"))
        {
          jelectrum_db = new jelectrum.db.cassandra.CassandraDB(config);
        }*/
        else
        {
          System.out.println("Unknown db_type: " + db_type);
          System.out.println("Try mongo or sql or leveldb or lobstack or slopbucket or rocksdb");
          System.exit(-1);
        }
        jelectrum_db.setRawBitcoinDataSource(bitcoin_rpc);

        
        block_store = new MapBlockStore(this);
        
        block_chain = new BlockChain(network_params, block_store);
        
        utxo_source = new SimpleUtxoMgr(this);

        notifier = new ElectrumNotifier(this);
        
        importer = new Importer(network_params, this, block_store);

        peer_manager = new PeerManager(this);

        stratum_server = new StratumServer(this, config);

        block_chain_cache = BlockChainCache.load(this);
        //block_chain_cache.undumbSelf(network_params, block_store);

        header_chunk_agent = new HeaderChunkAgent(this);

        jelectrum_db.setBlockChainCache(block_chain_cache);

        mem_pooler = new MemPooler(this);

    }


    public void start()
        throws Exception
    {
        //utxo_trie_mgr.getUtxoState();
        //if (config.getBoolean("bulk_import_enabled"))
        //{
        //  new BulkImporter(this);
        //}

        System.out.println("Updating block chain cache");
        block_chain_cache.update(this, block_store.getChainHead());
        
        utxo_source.start();


        System.out.println("Starting things");
        importer.start();
        notifier.start();

        stratum_server.setEventLog(event_log);

        stratum_server.start();
        peer_manager.start();

        mem_pooler.start();
        
        new BlockDownloadThread(this).start();


        /*System.out.println("Starting bitcoin peer download");


        peer_group = new PeerGroup(network_params, block_chain);

        peer_group.setMaxPeersToDiscoverCount(256);
        peer_group.setUseLocalhostPeerWhenPossible(false);

        peer_group.setMaxConnections(60);
        if (config.isSet("bitcoin_peer_host") && (config.isSet("bitcoin_peer_port")))
        {
          peer_group.addAddress(
            new PeerAddress(
              network_params,
              InetAddress.getByName(
                config.get("bitcoin_peer_host")),
                config.getInt("bitcoin_peer_port")));
        }

        if (config.isSet("bitcoin_peer_list"))
        {
          for(String peer : config.getList("bitcoin_peer_list"))
          {
            event_log.log("Adding additional bitcoin peer: " + peer);
            peer_group.addAddress(new PeerAddress(network_params,InetAddress.getByName(peer),8333));
          }
        }

        if (config.getBoolean("bitcoin_network_use_peers"))
        {
            peer_group.addPeerDiscovery(new DnsDiscovery(network_params));
        }
        ImportEventListener listener = new ImportEventListener(importer);
        peer_group.addOnTransactionBroadcastListener(listener);
        peer_group.addBlocksDownloadedEventListener(listener);
        peer_group.setMinBroadcastConnections(1);

        peer_group.waitForPeers(1);

        peer_group.start();
        Thread.sleep(2500);

        while (peer_group.numConnectedPeers() == 0)
        {
          event_log.alarm("No connected bitcoin peers - can't get new blocks or transactions");
          Thread.sleep(5000);
        }

        event_log.log("Connected bitcoin peers: " + peer_group.getConnectedPeers().size());
        for(Peer p : peer_group.getConnectedPeers())
        {
          event_log.log("Connected bitcoin peer: " + p);
        }
        event_log.log("Pending bitcoin peers: " + peer_group.getPendingPeers().size());
        for(Peer p : peer_group.getPendingPeers())
        {
          event_log.log("Pending bitcoin peer: " + p);
        }

        peer_group.downloadBlockChain();

        while(peer_group.getMostCommonChainHeight() > notifier.getHeadHeight())
        {
            Thread.sleep(5000);
        }

        new IrcBot(this,null).start();
        new IrcBot(this,"onion").start();

        */
        while(getBitcoinRPC().getBlockHeight() > notifier.getHeadHeight())
        {
          Thread.sleep(5000);
        }

        importer.setBlockPrintEvery(1);
        importer.disableRatePrinting();
        System.out.println("Block chain caught up");
        event_log.log("Block chain caught up");
        caught_up=true;
        

        header_chunk_agent.start();

        /*if (config.getBoolean("block_repo_saver"))
        {
          new BlockRepoSaver(this,100).start();
          new BlockRepoSaver(this,10).start();
        }*/

        /*while(true)
        {
          int peer_height = peer_group.getMostCommonChainHeight();
          int my_height = notifier.getHeadHeight();
          if (peer_height != my_height)
          {
            event_log.log("Peer height is: " + peer_height + " My height is: " + my_height);
            for(Peer p : peer_group.getConnectedPeers())
            {
              StringBuilder sb = new StringBuilder();
              VersionMessage vm = p.getPeerVersionMessage();
              sb.append("Peer:");
              sb.append(" " + vm.theirAddr);
              sb.append(" Height: " + p.getBestHeight());
              event_log.log(sb.toString());



            }


          }
            if (peer_group.getMostCommonChainHeight() > notifier.getHeadHeight()+3)
            {
                event_log.alarm("We are far behind.  Aborting.");
                System.exit(1);
            }

            Thread.sleep(25000);
        }*/
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

    public UtxoSource getUtxoSource()
    {
      return utxo_source;
    }
    public StratumServer getStratumServer()
    {
      return stratum_server;
    }
    public PeerManager getPeerManager()
    {
      return peer_manager;
    }
    public MemPooler getMemPooler()
    {
      return mem_pooler;
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

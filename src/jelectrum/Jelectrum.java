package jelectrum;

import com.google.bitcoin.store.H2FullPrunedBlockStore;
import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.params.MainNetParams;
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

public class Jelectrum
{
    public static void main(String args[]) throws Exception
    {
        new Jelectrum(new Config(args[0])).start();

    }

    private Config config;
    private JelectrumDB jelectrum_db;
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

    public Jelectrum(Config conf)
        throws Exception
    {
        network_params = MainNetParams.get();
        config = conf;


        config.require("bitcoin_network_use_peers");

        event_log = new EventLog(config);

        //jelectrum_db = new JelectrumDBMapDB(config);
        //jelectrum_db = new JelectrumDBDirect(config);
        //jelectrum_db = new JelectrumDBCloudData(config);
        jelectrum_db = new JelectrumDBMongo(config);
        
        block_store = new MapBlockStore(this);
        
        block_chain = new BlockChain(network_params, block_store);

        notifier = new ElectrumNotifier(this);
        
        importer = new Importer(network_params, this, block_store);

        stratum_server = new StratumServer(this, config);

        block_chain_cache = BlockChainCache.load(this);

        header_chunk_agent = new HeaderChunkAgent(this);

        bitcoin_rpc = new BitcoinRPC(config);
        bitcoin_rpc.testConnection();


    }

    public void start()
        throws Exception
    {
        System.out.println("Updating block chain cache");
        block_chain_cache.update(this, block_store.getChainHead());


        System.out.println("Starting things");
        importer.start();
        notifier.start();

        stratum_server.setEventLog(event_log);

        stratum_server.start();


        System.out.println("Starting peer download");


        peer_group = new PeerGroup(network_params, block_chain);

        peer_group.setMaxConnections(16);
        peer_group.addAddress(new PeerAddress(InetAddress.getByName(config.get("bitcoind_host")),8333));

        if (config.getBoolean("bitcoin_network_use_peers"))
        {
            peer_group.addPeerDiscovery(new DnsDiscovery(MainNetParams.get()));

            peer_group.addPeerDiscovery(new IrcDiscovery("#bitcoin"));

        }
        peer_group.addEventListener(new DownloadListener());
        peer_group.addEventListener(new ImportEventListener(jelectrum_db, importer));
        peer_group.setMinBroadcastConnections(1);

        peer_group.start();
        peer_group.downloadBlockChain();

        while(bitcoin_rpc.getBlockHeight() > notifier.getHeadHeight())
        {
            Thread.sleep(5000);
        }

        System.out.println("Block chain caught up");
        event_log.log("Block chain caught up");

        importer.setBlockPrintEvery(1);
        importer.disableRatePrinting();
        

        header_chunk_agent.start();


        while(true)
        {
            if (bitcoin_rpc.getBlockHeight() > notifier.getHeadHeight()+2)
            {
                System.out.println("We are far behind.  Aborting.");
                event_log.log("We are far behind.  Aborting.");
                System.exit(1);
            }

            Thread.sleep(25000);
        }
        /*peer_group.stop();
        jelectrum_db.commit();
        jelectrum_db.close();*/

    }

    public Config getConfig()
    {
        return config;
    }

    public Importer getImporter()
    {
        return importer;
    }

    public JelectrumDB getDB()
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

    public BlockStore getBlockStore()
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


    

}

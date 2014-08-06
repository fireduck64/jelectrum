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
        peer_group.addPeerDiscovery(new DnsDiscovery(MainNetParams.get()));

        peer_group.addPeerDiscovery(new IrcDiscovery("#bitcoin"));

        peer_group.addEventListener(new DownloadListener());
        peer_group.addEventListener(new ImportEventListener(jelectrum_db, importer));
        peer_group.setMinBroadcastConnections(2);

        peer_group.start();
        peer_group.downloadBlockChain();

        System.out.println("Block chain caught up");
        event_log.log("Block chain caught up");

        importer.setBlockPrintEvery(1);
        importer.disableRatePrinting();
        

        header_chunk_agent.start();

        /*peer_group.stop();
        jelectrum_db.commit();
        jelectrum_db.close();*/


        LinkedList<String> broadcast_list=new LinkedList<String>();

        broadcast_list.add("ff91b80282056722374b6bb74c6947b0496b7e9dd0b0be27fd7dc523ec913843");
        broadcast_list.add("be7e2ac2e4af7bc6cb61bef7b958c318932d5ac5bd14a7ccef111451e6d3a241");
        broadcast_list.add("da305e024043af44efb11118f76f2ae047f93e6dc3af4fdb8825b238fdf478b2");
        broadcast_list.add("efc8faa25be9f871d166612b8a59cea64d64cdddf1c31ecb67f8be68e914f70e");

        for(String tx_hash : broadcast_list)
        {
            System.out.println("TX: " + tx_hash);

            Transaction tx = jelectrum_db.getTransactionMap().get(new Sha256Hash(tx_hash)).getTx(network_params);

            System.out.println(tx);
            String tx_str = Hex.encodeHexString(tx.bitcoinSerialize());

            System.out.println(getBitcoinRPC().submitTransaction(tx_str));



        }





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

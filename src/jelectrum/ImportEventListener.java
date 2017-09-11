package jelectrum;

//import org.bitcoinj.core.listeners.AbstractPeerEventListener;
import org.bitcoinj.core.Peer;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.FilteredBlock;

import org.bitcoinj.core.listeners.OnTransactionBroadcastListener;
import org.bitcoinj.core.listeners.BlocksDownloadedEventListener;


public class ImportEventListener implements OnTransactionBroadcastListener,BlocksDownloadedEventListener
{
    private Importer importer;

    public ImportEventListener(Importer importer)
    {
        this.importer = importer;


    }
    @Override
    public void onBlocksDownloaded(Peer peer, Block block, FilteredBlock fblock, int blocksLeft)
    {
        importer.saveBlock(block);

    }

    @Override
    public void onTransaction(Peer peer, Transaction t)
    {
        importer.saveTransaction(t);
    }


}

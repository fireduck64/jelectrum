package jelectrum;

import org.bitcoinj.core.listeners.AbstractPeerEventListener;
import org.bitcoinj.core.Peer;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.FilteredBlock;


public class ImportEventListener extends AbstractPeerEventListener
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

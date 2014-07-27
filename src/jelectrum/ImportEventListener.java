package jelectrum;

import com.google.bitcoin.core.AbstractPeerEventListener;
import com.google.bitcoin.core.PeerEventListener;
import com.google.bitcoin.core.Peer;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.Transaction;


public class ImportEventListener extends AbstractPeerEventListener
{
    private JelectrumDB file_db;
    private Importer importer;

    public ImportEventListener(JelectrumDB file_db, Importer importer)
    {
        this.file_db = file_db;

        this.importer = importer;


    }
    @Override
    public void onBlocksDownloaded(Peer peer, Block block, int blocksLeft)
    {
        importer.saveBlock(block);

    }

    @Override
    public void onTransaction(Peer peer, Transaction t)
    {
        importer.saveTransaction(t);
    }


}

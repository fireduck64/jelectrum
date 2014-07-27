package jelectrum;

import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.NetworkParameters;


public class SerializedBlock implements java.io.Serializable
{
    private transient Block tx;

    private byte[] bytes;

    public SerializedBlock(Block tx)
    {
        this.tx = tx;
        bytes = tx.bitcoinSerialize();
    }

    public Block getBlock(NetworkParameters params)
    {
        if (tx != null) return tx;
        tx = new Block(params, bytes);
        return tx;
        
    }


}

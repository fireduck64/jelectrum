package jelectrum;

import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.NetworkParameters;


public class SerializedTransaction implements java.io.Serializable
{
    private transient Transaction tx;

    private byte[] bytes;

    private long saved_time=0L;

    public SerializedTransaction(Transaction tx)
    {
        this.tx = tx;
        bytes = tx.bitcoinSerialize();
        saved_time=System.currentTimeMillis();
    }

    public Transaction getTx(NetworkParameters params)
    {
        if (tx != null) return tx;
        tx = new Transaction(params, bytes);
        return tx;
        
    }

    public static Transaction scrubTransaction(NetworkParameters params, Transaction trans)
    {
        return new Transaction(params, trans.bitcoinSerialize());
    }

    public long getSavedTime()
    {
        return saved_time;
    }


}

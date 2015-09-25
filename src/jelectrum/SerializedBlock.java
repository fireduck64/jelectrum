package jelectrum;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import com.google.protobuf.ByteString;


public class SerializedBlock implements java.io.Serializable
{
  public static final long serialVersionUID = 1280282305786765588L;

    private transient Block tx;

    private byte[] bytes;

    public SerializedBlock(Block tx)
    {
        this.tx = tx;
        bytes = tx.bitcoinSerialize();
    }
    public SerializedBlock(ByteString bytes)
    {
      this.bytes = bytes.toByteArray();
    }

    public Block getBlock(NetworkParameters params)
    {
        if (tx != null) return tx;
        tx = new Block(params, bytes);
        return tx;
        
    }
    public byte[] getBytes()
    {
      return bytes;
    }


}

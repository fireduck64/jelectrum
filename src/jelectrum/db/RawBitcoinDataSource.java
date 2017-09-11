package jelectrum.db;
import jelectrum.SerializedTransaction;
import jelectrum.SerializedBlock;
import org.bitcoinj.core.Sha256Hash;

public interface RawBitcoinDataSource
{
  public SerializedTransaction getTransaction(Sha256Hash hash);
  public SerializedBlock getBlock(Sha256Hash hash);
}

package jelectrum;


import org.bitcoinj.core.Address;
import org.bitcoinj.core.TransactionOutPoint;
import java.util.Collection;
import org.bitcoinj.core.Sha256Hash;


public interface UtxoSource
{
  public Collection<TransactionOutPoint> getUnspentForAddress(Address a);

  public void notifyBlock(boolean wait_for_it, Sha256Hash wait_for_block);

  public boolean isUpToDate();

  public void start();

}

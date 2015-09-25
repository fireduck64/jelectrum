package jelectrum;

import org.bitcoinj.core.Sha256Hash;

import org.bitcoinj.core.Block;

public class UtxoStatus implements java.io.Serializable
{
  private static final long serialVersionUID = 859944923280822401L;

  public Sha256Hash block_hash;
  public Sha256Hash prev_block_hash;
  public boolean complete;

  public UtxoStatus()
  {

  }

  public UtxoStatus(Sha256Hash block_hash)
  {
    this.block_hash = block_hash;

    this.complete = true;
  }

  public UtxoStatus(Sha256Hash block_hash, Sha256Hash prev_block_hash)
  {
    this.block_hash = block_hash;
    this.prev_block_hash = prev_block_hash;
    this.complete = false;
  }

  public Sha256Hash getBlockHash(){return block_hash;}
  public boolean isConsistent(){return complete;}
  public Sha256Hash getPrevBlockHash(){return prev_block_hash;}



}

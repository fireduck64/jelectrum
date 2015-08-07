package jelectrum;

import com.google.bitcoin.core.Sha256Hash;

import com.google.bitcoin.core.Block;

public class UtxoStatus implements java.io.Serializable
{
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

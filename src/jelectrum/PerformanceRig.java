package jelectrum;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import java.util.HashSet;

import java.io.*;
import jelectrum.proto.Kraken.ProtoBlockSummary;
import com.google.protobuf.ByteString;

public class PerformanceRig
{
  public static void main(String args[]) throws Exception
  {
    Config config = new Config(args[0]);
    new PerformanceRig(config);

  }

  public PerformanceRig(Config config)
    throws Exception
  {
    Jelectrum jelly = new Jelectrum(config);

    int start_height = 375000;
    int items = 1000;


    TimeRecord tr = new TimeRecord();
    TimeRecord.setSharedRecord(tr);
    for(int h = start_height; h < start_height + items; h++)
    {
      long t1=0;
      Sha256Hash hash = jelly.getBlockChainCache().getBlockHashAtHeight(h);

      t1 = System.nanoTime();
      SerializedBlock s_blk = jelly.getDB().getBlockMap().get(hash);
      TimeRecord.record(t1, "get_serialized_block");

      t1 = System.nanoTime();
      Block blk = s_blk.getBlock( jelly.getNetworkParameters() );
      int sz = blk.getTransactions().size();
      HashSet<Sha256Hash> hashes = new HashSet<>();
      for(Transaction tx : blk.getTransactions())
      {
        hashes.add(tx.getHash());
      }
      TimeRecord.record(t1, "get_block");

      t1 = System.nanoTime();
      BlockSummary blk_sum = jelly.getDB().getBlockSummaryMap().get(hash);
      TimeRecord.record(t1, "get_block_summary");

      byte[] block_summary_serial = null;
    
      t1 = System.nanoTime();
      {
        ByteArrayOutputStream b_out = new ByteArrayOutputStream();
        ObjectOutputStream o_out = new ObjectOutputStream(b_out);
        o_out.writeObject(blk_sum);
        o_out.flush();

        byte[] b = b_out.toByteArray();
        TimeRecord.record(t1, "serialize_block_summary_size", b.length);
        block_summary_serial = b;
      }
      TimeRecord.record(t1, "serialize_block_summary");

      t1 = System.nanoTime();
      {
        ByteArrayInputStream b_in=new ByteArrayInputStream(block_summary_serial);
        ObjectInputStream o_in = new ObjectInputStream(b_in);

        BlockSummary blk_sum_read = (BlockSummary) o_in.readObject();
      }
      TimeRecord.record(t1, "deserialize_block_summary");

      t1 = System.nanoTime();
      ProtoBlockSummary proto_blk = blk_sum.getProto();
      TimeRecord.record(t1, "proto_create");

      t1 = System.nanoTime();
      ByteString proto_bytes = proto_blk.toByteString();
      TimeRecord.record(t1, "serialize_proto");
      TimeRecord.record(t1, "serialize_proto_size", proto_bytes.size());

      t1 = System.nanoTime();
      ProtoBlockSummary proto_blk_read = ProtoBlockSummary.parseFrom(proto_bytes);
      TimeRecord.record(t1, "deserialize_proto");
      


    }

    tr.printReport(System.out);


  }
    
}

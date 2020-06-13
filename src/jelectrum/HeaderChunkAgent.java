package jelectrum;

import org.apache.commons.codec.binary.Hex;
import java.io.ByteArrayOutputStream;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Sha256Hash;

import org.json.JSONObject;
import org.json.JSONArray;
import snowblossom.lib.HexUtil;
import com.google.protobuf.ByteString;


public class HeaderChunkAgent extends Thread
{
    private Jelectrum jelly;
    private volatile int height;
    private int last_saved_height;

    public static final int MAX_RETURN=5000;
    public static final int BLOCK_HEADER_SIZE=160;


    public HeaderChunkAgent(Jelectrum jelly)
    {
        this.jelly = jelly;
        setName("HeaderChunkAgent");
        setDaemon(true);

    }

    public String getChunk(int index)
    {
        return jelly.getDB().getHeaderChunkMap().get(index);
    }

    public JSONObject getHeaders(int start, int count, int cp)
    {
      JSONObject result = new JSONObject();

      result.put("max", MAX_RETURN);
      count = Math.min(count, MAX_RETURN);
      count = Math.min(count, last_saved_height - start);

      int chunk_no = -1;
      String chunk_data = null;

      StringBuilder sb = new StringBuilder();

      String block_header = null;

      for(int i=start; i<start+count; i++)
      {
        int needed_chunk = i / 2016;
        if (chunk_no != needed_chunk)
        {
          chunk_no = needed_chunk;
          chunk_data = getChunk(chunk_no);
        }
        int idx_in_chunk = i % 2016;

        // This is a lot of string slicing.  We can make this faster by grabing a whole range, but whatever.
        block_header = chunk_data.substring(idx_in_chunk * BLOCK_HEADER_SIZE, idx_in_chunk * BLOCK_HEADER_SIZE + BLOCK_HEADER_SIZE);
        sb.append(block_header);
      }

      
      result.put("hex", sb.toString());

      if (cp >= 0)
      {

        ByteString last_block_header = HexUtil.hexStringToBytes(block_header);
        Sha256Hash last_block_hash = Util.swapEndian(Util.hashDoubleBs(last_block_header));


        jelly.getBlockChainCache().populateBlockProof(last_block_hash, start+count-1, cp, result);

      }


      return result;
    }

    public void poke(int new_height)
    {
        height = new_height;
        synchronized(this)
        {
            this.notifyAll();
        }
       
    }

    public void run()
    {
        boolean first=true;

        while(true)
        {
            try
            {
                if (first)
                {
                    height = jelly.getBlockStore().getChainHead().getHeight();
                    first=false;
                }

                doPass();
            }
            catch(Throwable t)
            {
                t.printStackTrace();
            }
            try
            {
                long t1 = System.currentTimeMillis();
                synchronized(this)
                {
                    this.wait();
                }
                if (System.currentTimeMillis() - t1 < 5000)
                {
                  Thread.sleep(5000); //Always sleep a little, avoid thrashing these chunks
                  // while we are still syncing
                }
            }
            catch(java.lang.InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }

    private void doPass()
        throws org.bitcoinj.store.BlockStoreException
    {
        int h = height;
        for(int i=0; i<h; i+=2016)
        {
            int index = i / 2016;
            //System.out.println("index: " + index+ " i: " + i + " h: " + h);
            //if (i+2016 <= h)
            {
                checkChunk(i/2016);
                last_saved_height = i+2016;
            }
        }
    }

    private void checkChunk(int index)
        throws org.bitcoinj.store.BlockStoreException
    {
        {
          String old = jelly.getDB().getHeaderChunkMap().get(index);
          if ((old != null) && (old.length() == 2 * 2016 * 80)) return;
          // TODO - not impossible that a reorg could result in a chunk of the correct size
          // being written but the last part needing to be re-written because of a reorg.
          // Fix by extracting last block header from chunk, getting block hash and making sure
          // that is still in main chain
        }

        jelly.getEventLog().log("Building header chunk " + index);

        ByteArrayOutputStream b_out = new ByteArrayOutputStream();
        int start = index * 2016;
        for(int i=0; i<2016; i++)
        {
            if (start+i <= height)
            {
                Sha256Hash hash = jelly.getBlockChainCache().getBlockHashAtHeight(start+i);
                if (hash == null) {
                  int h = start+i;
                  System.out.println("unable to find hash for height: " + h);
                }
                StoredBlock blk = jelly.getBlockStore().get(hash);
                byte[] b = blk.getHeader().bitcoinSerialize();

                b_out.write(b,0,80);
            }

        }
        byte[] buff = b_out.toByteArray();

        String chunk = new String(Hex.encodeHex(buff));
        
        jelly.getDB().getHeaderChunkMap().put(index, chunk);

    }



}

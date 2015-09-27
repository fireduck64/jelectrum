package jelectrum;

import org.bitcoinj.core.Sha256Hash;

import java.text.DecimalFormat;
import java.util.Map;
import jelectrum.proto.Blockrepo;
import com.google.protobuf.ByteString;
import org.json.JSONObject;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;


public class BlockRepoSaver extends Thread
{
  private int BLOCKS_PER_CHUNK=100;
  private Jelectrum jelly;

  private AmazonS3Client s3;
  private String bucket;

  public BlockRepoSaver(Jelectrum jelly, int BLOCKS_PER_CHUNK)
  {
    this.BLOCKS_PER_CHUNK = BLOCKS_PER_CHUNK;
    this.jelly = jelly;

    setName("BlockRepoSaver/" + BLOCKS_PER_CHUNK);
    setDaemon(true);

    Config config = jelly.getConfig();
    config.require("block_repo_saver_bucket");
    config.require("block_repo_saver_aws_id");
    config.require("block_repo_saver_aws_key");

    s3 = new AmazonS3Client(new BasicAWSCredentials(config.get("block_repo_saver_aws_id"), config.get("block_repo_saver_aws_key")));
    bucket = config.get("block_repo_saver_bucket");

  }

  public void run()
  {
    while(true)
    {
      try
      {
        doUploadRun();
        saveFeeEstimates();

      }
      catch(Throwable t)
      {
        jelly.getEventLog().alarm("BlockRepoSaver error: " + t);
        t.printStackTrace();
      }
      try{ sleep(600000); } catch(Throwable t){}
    }

  }

  private void doUploadRun()
  {
    int head_height = jelly.getElectrumNotifier().getHeadHeight();
    Map<String, Object> special_object_map = jelly.getDB().getSpecialObjectMap();

    int last_saved=-1;

    for(int start=0; start+BLOCKS_PER_CHUNK<=head_height; start+=BLOCKS_PER_CHUNK)
    {
      int end_block = start + BLOCKS_PER_CHUNK - 1;
      Sha256Hash end_hash = jelly.getBlockChainCache().getBlockHashAtHeight(end_block);
      String key = "blockchunk/" +BLOCKS_PER_CHUNK+"/" + start;

      Sha256Hash db_hash = null;
      try
      {
        db_hash = (Sha256Hash) special_object_map.get(key);
      }
      catch(Throwable t)
      {
        special_object_map.put(key, end_hash);
        db_hash = end_hash;
      }

      if ((db_hash == null) || (!db_hash.equals(end_hash)))
      {

        Blockrepo.BitcoinBlockPack.Builder pack_builder = Blockrepo.BitcoinBlockPack.newBuilder();
        pack_builder.setNewHeadHash(end_hash.toString());
        pack_builder.setStartHeight(start);

        for(int i=start; i<=end_block; i++)
        {
          Sha256Hash hash = jelly.getBlockChainCache().getBlockHashAtHeight(i);
          SerializedBlock blk = jelly.getDB().getBlockMap().get(hash);
          Blockrepo.BitcoinBlock.Builder blk_builder = Blockrepo.BitcoinBlock.newBuilder();

          blk_builder.setHeight(i);
          blk_builder.setHash(hash.toString());
          blk_builder.setBlockData(ByteString.copyFrom(blk.getBytes()));

          pack_builder.addBlocks(blk_builder.build());
        }

        Blockrepo.BitcoinBlockPack pack = pack_builder.build();

        ByteString bytes = pack.toByteString();
        ByteString c_data = ByteString.copyFrom(lobstack.ZUtil.compress(bytes.toByteArray()));

        saveFile(key, c_data, 86400*7);

        special_object_map.put(key, end_hash);
        jelly.getEventLog().log("BlockRepoSaver"+BLOCKS_PER_CHUNK+" done chunk: " + start + " to " + end_block + " - " + bytes.size() + " / " + c_data.size());
        last_saved=end_block;
      }
    }

    if (last_saved > 0)
    {
      String data_str="" + last_saved;
      saveFile("blockchunk/" +BLOCKS_PER_CHUNK+"/max", ByteString.copyFromUtf8(data_str), 300);
    }

  }

  private void saveFeeEstimates()
    throws java.io.IOException, org.json.JSONException
  {

    JSONObject obj = new JSONObject();
    obj.put("updated", System.currentTimeMillis());


    JSONObject fees = new JSONObject();

    for(int i=1; i<120; i++)
    {
      double fee=jelly.getBitcoinRPC().getFeeEstimate(i);


      fees.put("" + i, fee);

    }
    obj.put("fees", fees);

    saveFile("fee_estimates", ByteString.copyFromUtf8(obj.toString(2) +"\n"), 30);
  }

  private void saveFile(String key, ByteString data, int cache_seconds)
  {
    ObjectMetadata omd = new ObjectMetadata();
    omd.setCacheControl("max-age=" + cache_seconds);
    omd.setContentLength(data.size());


    PutObjectRequest put = new PutObjectRequest(bucket, key, data.newInput(), omd);
    put.setCannedAcl(CannedAccessControlList.PublicRead);
    put.setStorageClass(com.amazonaws.services.s3.model.StorageClass.StandardInfrequentAccess.toString());

    s3.putObject(put);

  }

}

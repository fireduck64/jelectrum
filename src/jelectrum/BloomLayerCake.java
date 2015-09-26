package jelectrum;

import java.io.File;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map;

import bloomtime.Bloomtime;
import java.util.ArrayList;
import java.util.Collection;

import com.google.protobuf.ByteString;

public class BloomLayerCake
{
  private File dir;
  private int max_blocks;
  private ArrayList<LayerInfo> layers;

  public BloomLayerCake(File dir, int max_blocks)
      throws Exception
  {
    this.max_blocks = max_blocks;
    this.dir = dir;
    dir.mkdirs();

    layers = new ArrayList<>();


    // If any of these values are changed, the entire bloom filter needs to be recreated
    layers.add(new LayerInfo(0, 10000, 0.03));
    layers.add(new LayerInfo(1, 400, 0.03));
    layers.add(new LayerInfo(2, 16, 0.03));
    layers.add(new LayerInfo(3, 1, 0.03));
  }

  /**
   * Using this, the string hashes differently on each layer
   * making multiple layers of false positives less likely
   * (probably)
   */
  private ByteString getDataForLayer(String addr, int layer)
  {
    String s = addr + "_l" + layer;
    return ByteString.copyFromUtf8(s);

  }

  public void addAddresses(int block_height, Collection<String> lst)
  {
    for(LayerInfo li : layers)
    {
      int slice = li.mapBlockHeightToSlice(block_height);

      for(String s : lst)
      {
        li.bloomtime.accumulateBits(slice, getDataForLayer(s, li.layer_no));
      }

    }
  }
  public void flush()
  {
    for(LayerInfo li : layers)
    {
      li.bloomtime.flushBits();
    }
  }

  public Set<Integer> getBlockHeightsForAddress(String address)
  {
    TreeMap<Integer, Integer> block_ranges=new TreeMap<>();

    for(int l=0; l<layers.size(); l++)
    {
      ByteString data = getDataForLayer(address, l);
      if (l == 0)
      {
        Set<Integer> slices = layers.get(l).bloomtime.getMatchingSlices(data);
        for(int slice : slices)
        {
          int low = layers.get(l).mapSliceIntoBlockHeightLow(slice);
          int high = layers.get(l).mapSliceIntoBlockHeightHigh(slice);
          block_ranges.put(low, high);
        }
      }
      else
      {
        TreeMap<Integer, Integer> next_block_ranges=new TreeMap<>();

        for(Map.Entry<Integer, Integer> me : block_ranges.entrySet())
        {
          int in_low = me.getKey();
          int in_high = me.getValue();
          int in_slice_low = layers.get(l).mapBlockHeightToSlice(in_low);
          int in_slice_high = layers.get(l).mapBlockHeightToSlice(in_high);
          //System.out.println("At layer " + l + " checking " + in_low + " to " + in_high);
          Set<Integer> slices = layers.get(l).bloomtime.getMatchingSlices(data, in_slice_low, in_slice_high);
          for(int slice : slices)
          {
            int low = layers.get(l).mapSliceIntoBlockHeightLow(slice);
            int high = layers.get(l).mapSliceIntoBlockHeightHigh(slice);
            next_block_ranges.put(low, high);
          }
        }
        block_ranges = next_block_ranges;
      }
    }

    return block_ranges.keySet();
  }
  

  public class LayerInfo
  {
    public static final int ESTIMATE_KEYS_PER_BLOCK=10000;
    private int blocks_per_layer;
    protected Bloomtime bloomtime;
    protected int layer_no;


    public LayerInfo(int layer_no, int blocks, double prob)
      throws Exception
    {
      blocks_per_layer = blocks;
      this.layer_no = layer_no;
      double estimate_keys = blocks * ESTIMATE_KEYS_PER_BLOCK;
      int bit_len = (int)Math.round(estimate_keys * Math.log(prob) / Math.log(1 / Math.pow(2, Math.log(2))));
      int slices = max_blocks / blocks;
      if (max_blocks % blocks != 0) slices++;
      while (slices % 8 != 0) slices++;


      int hash_count = (int)Math.round(Math.log(2) * bit_len / estimate_keys);

      bloomtime = new Bloomtime(new File(dir, "layer_" + layer_no), slices, bit_len, hash_count);
    }
    
    public int mapBlockHeightToSlice(int height)
    {
      return height / blocks_per_layer;
    }
    public int mapSliceIntoBlockHeightHigh(int slice)
    {
      return blocks_per_layer * (slice +1);

    }
    public int mapSliceIntoBlockHeightLow(int slice)
    {
      return blocks_per_layer * slice;
    }


  }

}

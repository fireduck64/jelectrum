package jelectrum.db;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.LinkedList;
import java.text.DecimalFormat;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Block;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.AbstractMap.SimpleEntry;
import static jelectrum.db.ObjectConversionMap.ConversionMode.*;

import jelectrum.SerializedTransaction;
import jelectrum.SerializedBlock;
import jelectrum.UtxoTrieNode;
import jelectrum.Config;
import jelectrum.BlockChainCache;
import jelectrum.TXUtil;

import com.google.protobuf.ByteString;

public interface DBFace
{

    public Map<Sha256Hash, StoredBlock> getBlockStoreMap();
    public Map<String, StoredBlock> getSpecialBlockStoreMap();
    //public Map<Sha256Hash, SerializedTransaction> getTransactionMap();
    //public Map<Sha256Hash, SerializedBlock> getBlockMap();
    public Map<Sha256Hash, String> getBlockSavedMap();
    public Map<String, Object> getSpecialObjectMap();
    public Map<Integer, String> getHeaderChunkMap();
    public Map<Integer, Sha256Hash> getHeightMap();
    public Map<String, UtxoTrieNode> getUtxoTrieMap();
    public DBMapMutationSet getUtxoSimpleMap();

    public void addPublicKeysToTxMap(Collection<ByteString> publicKeys, Sha256Hash hash);
    public void addPublicKeysToTxMap(Collection<Map.Entry<ByteString, Sha256Hash> > lst);
    public Set<Sha256Hash> getPublicKeyToTxSet(ByteString publicKey);
    
    /*public void addAddressesToTxMap(Collection<String> addresses, Sha256Hash hash);
    public void addAddressesToTxMap(Collection<Map.Entry<String, Sha256Hash> > lst);
    public Set<Sha256Hash> getAddressToTxSet(String address);*/

    public SerializedTransaction getTransaction(Sha256Hash hash);
    public SerializedBlock getBlock(Sha256Hash hash);


    /**
     * Add address and tx mappings a block at a time, if supported
     */
    public void addBlockThings(int height, Block blk);

    /**
     * If returns true, then the DB needs transactions added to the transaction map,
     * blocks to the block map
     * and addAddressesToTxMap() and addTxsToBlockMap() to be called.  If false,
     * then addBlockThings() covers it. */
     @Deprecated
    public boolean needsDetails();


    //public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block);
    //public void addTxsToBlockMap(Collection<Sha256Hash> txs, Sha256Hash block);
    //public void addTxsToBlockMap(Collection<Map.Entry<Sha256Hash, Sha256Hash> > lst);
    //public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx);

    public void commit();
    public void setBlockChainCache(BlockChainCache block_chain_cache);

    public TXUtil getTXUtil();

    public void setRawBitcoinDataSource(RawBitcoinDataSource rawSource);
}

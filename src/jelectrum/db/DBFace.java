package jelectrum.db;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.LinkedList;
import java.text.DecimalFormat;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.AbstractMap.SimpleEntry;
import static jelectrum.db.ObjectConversionMap.ConversionMode.*;

import jelectrum.SerializedTransaction;
import jelectrum.SerializedBlock;
import jelectrum.UtxoTrieNode;
import jelectrum.Config;


public interface DBFace
{

    public Map<Sha256Hash, StoredBlock> getBlockStoreMap();
    public Map<String, StoredBlock> getSpecialBlockStoreMap();
    public Map<Sha256Hash,SerializedTransaction> getTransactionMap();
    public Map<Sha256Hash, SerializedBlock> getBlockMap();
    public Map<Sha256Hash, String> getBlockRescanMap();
    public Map<String, Object> getSpecialObjectMap();
    public Map<Integer, String> getHeaderChunkMap();
    public Map<String, UtxoTrieNode> getUtxoTrieMap();

    public void addAddressesToTxMap(Collection<String> addresses, Sha256Hash hash);
    public void addAddressesToTxMap(Collection<Map.Entry<String, Sha256Hash> > lst);
    public Set<Sha256Hash> getAddressToTxSet(String address);


    public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block);
    public void addTxsToBlockMap(Collection<Sha256Hash> txs, Sha256Hash block);
    public void addTxsToBlockMap(Collection<Map.Entry<Sha256Hash, Sha256Hash> > lst);
    public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx);

    public void commit();

}

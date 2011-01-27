package org.eclipse.jgit.storage.hbase;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;

public class HRepositoryIndexTable implements RepositoryIndexTable {
  public static final byte[] REPOSITORY_INDEX_FAMILY =
    Bytes.toBytes("repository_index");
  public static final byte[] ID = Bytes.toBytes("id");

  private HBaseDatabase db;

  public HRepositoryIndexTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public RepositoryKey get(RepositoryName name) throws DhtException,
      TimeoutException {
    HTableInterface table = db.getTable();
    try {
      Result result = 
        table.get(new Get(name.toBytes())
          .addColumn(REPOSITORY_INDEX_FAMILY, ID));
      return RepositoryKey.fromBytes(
        result.getValue(REPOSITORY_INDEX_FAMILY, ID));
    } catch (IOException e) {
      throw new DhtException(e);
    } finally {
      db.putTable(table);
    }
  }

  @Override
  public void putUnique(RepositoryName name, RepositoryKey key)
      throws DhtException, TimeoutException {
    HTableInterface table = db.getTable();
    try {
      table.put(new Put(name.toBytes()).add(REPOSITORY_INDEX_FAMILY, ID,
        key.toBytes()));
    } catch (IOException e) {
      throw new DhtException(e);
    } finally {
      db.putTable(table);
    }
  }
}

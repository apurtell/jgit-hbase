/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.eclipse.jgit.storage.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefData;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RefTransaction;

public class HRefTable implements RefTable {
  public static final byte[] REF_FAMILY = Bytes.toBytes("ref");

  private final HBaseDatabase db;

  public HRefTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public Map<RefKey, RefData> getAll(Context options, RepositoryKey repository)
      throws DhtException {
    Map<RefKey, RefData> m = new HashMap<RefKey, RefData>();
    HTableInterface table = db.getTable();
    try {
      Get get = new Get(repository.toBytes());
      get.addFamily(REF_FAMILY);
      Result result = table.get(get);
      Map<byte[],byte[]> familyMap = result.getFamilyMap(REF_FAMILY);
      if (familyMap != null) {
        for (Map.Entry<byte[],byte[]> e: familyMap.entrySet()) {
          m.put(RefKey.fromBytes(e.getKey()), RefData.fromBytes(e.getValue()));
        }
      }
    } catch (IOException e) {
      throw new DhtException(e);
    } finally {
      db.putTable(table);
    }
    return m;
  }

  @Override
  public RefTransaction newTransaction(final RefKey newKey)
      throws DhtException, TimeoutException {
    final RefData old;
    HTableInterface table = db.getTable();
    try {
      Get get = new Get(newKey.getRepositoryKey().toBytes());
      get.addColumn(REF_FAMILY, newKey.toBytes());
      Result result = table.get(get);
      if (!result.isEmpty()) {
        old = RefData.fromBytes(result.list().get(0).getValue());
      } else {
        old = null;
      }
    } catch (IOException e) {
      throw new DhtException(e);
    } finally {
      db.putTable(table);
    }
      
    return new RefTransaction() {
      @Override
      public RefData getOldData() {
        return old;
      }

      @Override
      public boolean compareAndPut(RefData newData) throws DhtException {
        HTableInterface table = db.getTable();
        try {
          Put put = new Put(newKey.getRepositoryKey().toBytes());
          put.add(REF_FAMILY, newKey.toBytes(), newData.toBytes());
          return table.checkAndPut(newKey.getRepositoryKey().toBytes(),
            REF_FAMILY, newKey.toBytes(),
            old != null ? old.toBytes() : null,
            put);
        } catch (IOException e) {
          throw new DhtException(e);
        } finally {
          db.putTable(table);
        }        
      }

      public boolean compareAndRemove() throws DhtException,
          TimeoutException {
        HTableInterface table = db.getTable();
        try {
          Delete delete = new Delete(newKey.getRepositoryKey().toBytes());
          delete.deleteColumn(REF_FAMILY, newKey.toBytes());
          return table.checkAndDelete(newKey.getRepositoryKey().toBytes(),
            REF_FAMILY, newKey.toBytes(),
            old != null ? old.toBytes() : null,
            delete);
        } catch (IOException e) {
          throw new DhtException(e);
        } finally {
          db.putTable(table);
        }        
      }

      @Override
      public void abort() {
      }
    };
  }
}

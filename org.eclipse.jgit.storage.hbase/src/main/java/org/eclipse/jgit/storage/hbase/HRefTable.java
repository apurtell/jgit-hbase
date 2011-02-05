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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefData;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RefTransaction;

public class HRefTable implements RefTable {
  public static final byte[] REF_FAMILY = Bytes.toBytes("ref");

  private final HBaseDatabase db;

  public HRefTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public Map<RefKey, RefData> getAll(RepositoryKey repository)
      throws DhtException, TimeoutException {
    Map<RefKey, RefData> map = new HashMap<RefKey, RefData>();
    HTableInterface table = db.getTable();
    try {
      Result result = table.get(new Get(repository.asByteArray())
        .addFamily(REF_FAMILY));
      for (KeyValue kv: result.raw()) {
        map.put(RefKey.fromBytes(kv.getQualifier()),
          RefData.fromBytes(kv.getValue()));        
      }
    } catch (IOException e) {
      throw new DhtException(e);
    } finally {
      db.putTable(table);
    }
    return map;
  }

  @Override
  public RefTransaction newTransaction(final RefKey newKey) 
      throws DhtException, TimeoutException {
    final RefData old;
    HTableInterface table = db.getTable();
    try {
      Result result = 
        table.get(new Get(newKey.getRepositoryKey().asByteArray())
          .addColumn(REF_FAMILY, newKey.asByteArray()));
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
      public boolean compareAndPut(RefData newData) throws DhtException,
          TimeoutException {
        HTableInterface table = db.getTable();
        try {
          Put put = new Put(newKey.getRepositoryKey().asByteArray());
          put.add(REF_FAMILY, newKey.asByteArray(), newData.asByteArray());
          return table.checkAndPut(newKey.getRepositoryKey().asByteArray(),
            REF_FAMILY, newKey.asByteArray(),
            old != null ? old.asByteArray() : null,
            put);
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

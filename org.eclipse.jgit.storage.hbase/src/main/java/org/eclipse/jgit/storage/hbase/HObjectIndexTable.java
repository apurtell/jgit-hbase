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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.ChunkLink;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.ObjectIndexKey;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class HObjectIndexTable implements ObjectIndexTable {
  public static final byte[] OBJECT_INDEX_FAMILY = Bytes.toBytes("index");

  private final HBaseDatabase db;

  public HObjectIndexTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public void get(final Set<ObjectIndexKey> objects,
      final AsyncCallback<Map<ObjectIndexKey, Collection<ChunkLink>>> callback) {
    db.submit(new Runnable() {
      @Override
      public void run() {
        List<Get> gets = new ArrayList<Get>();
        for (ObjectIndexKey key: objects) {
          gets.add(new Get(key.asByteArray()).addFamily(OBJECT_INDEX_FAMILY));
        }
        HTableInterface table = db.getTable();
        try {
          callback.onSuccess(parseChunks(table.get(gets)));
        } catch (Throwable err) {
          callback.onFailure(new DhtException(err));
        } finally {
          db.putTable(table);
        }
      }
    });
  }

  protected Map<ObjectIndexKey, Collection<ChunkLink>> parseChunks(
      Result[] results) {
    Map<ObjectIndexKey, Collection<ChunkLink>> map = 
      new HashMap<ObjectIndexKey, Collection<ChunkLink>>();
    for (Result result: results) {
      if (result == null || result.getRow() == null) {
        continue;
      }
      ObjectIndexKey key = ObjectIndexKey.fromBytes(result.getRow());
      Collection<ChunkLink> links = new ArrayList<ChunkLink>();
      List<KeyValue> kvs = result.list();
      for (KeyValue kv: kvs) {
        links.add(ChunkLink.fromBytes(
          ChunkKey.fromBytes(kv.getQualifier()),
          kv.getValue(),
          kv.getTimestamp()));
      }
      map.put(key, links);
    }
    return map;
  }

  @Override
  public void add(ObjectIndexKey objId, ChunkLink chunk, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      wb.put(new Put(objId.asByteArray()).add(OBJECT_INDEX_FAMILY,
        chunk.getChunkKey().asByteArray(), chunk.getTime(),
        chunk.asByteArray()));
    } catch (Throwable err) {
      throw new DhtException(err);
    }
  }

  @Override
  public void replace(ObjectIndexKey objId, ChunkLink chunk, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      wb.delete(new Delete(objId.asByteArray()));
      wb.put(new Put(objId.asByteArray()).add(OBJECT_INDEX_FAMILY,
        chunk.getChunkKey().asByteArray(), chunk.getTime(),
        chunk.asByteArray()));
    } catch (Throwable err) {
      throw new DhtException(err);
    }
  }
}

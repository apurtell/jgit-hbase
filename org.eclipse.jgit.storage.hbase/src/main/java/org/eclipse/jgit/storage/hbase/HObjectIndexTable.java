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
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.ObjectIndexKey;
import org.eclipse.jgit.storage.dht.ObjectInfo;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class HObjectIndexTable implements ObjectIndexTable {
  public static final byte[] OBJECT_INDEX_FAMILY = Bytes.toBytes("object_index");

  private final HBaseDatabase db;

  public HObjectIndexTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public void get(Context options,
      final Set<ObjectIndexKey> objects,
      final AsyncCallback<Map<ObjectIndexKey, Collection<ObjectInfo>>> callback) {
    db.submit(new Runnable() {
      @Override
      public void run() {
        List<Get> gets = new ArrayList<Get>();
        for (ObjectIndexKey key: objects) {
          gets.add(new Get(key.toBytes()).addFamily(OBJECT_INDEX_FAMILY));
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

      Map<ObjectIndexKey, Collection<ObjectInfo>> parseChunks(Result[] results)
          throws IOException {
        Map<ObjectIndexKey, Collection<ObjectInfo>> m =
          new HashMap<ObjectIndexKey, Collection<ObjectInfo>>();
        if (results != null) {
          for (Result result: results) {
            if (result == null) {
              assert false;
              continue;
            }
            if (result.getRow() == null) {
              continue;
            }
            ObjectIndexKey key = ObjectIndexKey.fromBytes(result.getRow());
            Collection<ObjectInfo> objects = new ArrayList<ObjectInfo>();
            List<KeyValue> kvs = result.list();
            assert(kvs != null);
            for (KeyValue kv: kvs) {
              objects.add(ObjectInfo.fromBytes(
                ChunkKey.fromShortBytes(key, kv.getQualifier()),
                kv.getValue(),
                kv.getTimestamp()));
            }
            m.put(key, objects);
          }
        }
        return m;
      }
    });
  }

  @Override
  public void add(ObjectIndexKey objId, ObjectInfo link, WriteBuffer buffer)
      throws DhtException {
    try {
      HWriteBuffer wb = (HWriteBuffer) buffer;
      ChunkKey key = link.getChunkKey();
      wb.put(
        new Put(objId.toBytes())
          .add(OBJECT_INDEX_FAMILY, key.toShortBytes(), link.toBytes()));
    } catch (Throwable err) {
      throw new DhtException(err);
    }
  }

  @Override
  public void replace(ObjectIndexKey objId, ObjectInfo link, WriteBuffer buffer)
      throws DhtException {
    try {
      HWriteBuffer wb = (HWriteBuffer) buffer;
      wb.delete(new Delete(objId.toBytes()));
      ChunkKey key = link.getChunkKey();
      wb.put(
        new Put(objId.toBytes())
          .add(OBJECT_INDEX_FAMILY, key.toShortBytes(), link.toBytes()));
    } catch (Throwable err) {
      throw new DhtException(err);
    }
  }

  @Override
  public void remove(ObjectIndexKey objId, ChunkKey chunk, WriteBuffer buffer)
      throws DhtException {
    try {
      HWriteBuffer wb = (HWriteBuffer) buffer;
      wb.delete(new Delete(objId.toBytes())
        .deleteColumn(OBJECT_INDEX_FAMILY, chunk.toShortBytes()));
    } catch (Throwable err) {
      throw new DhtException(err);
    }
  }
}

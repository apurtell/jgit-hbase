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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.ObjectListChunk;
import org.eclipse.jgit.storage.dht.ObjectListChunkKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.ObjectListTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class HObjectListTable implements ObjectListTable {
  public static final byte[] OBJECT_LIST_FAMILY = Bytes.toBytes("object_list");
  public static final byte[] CHUNK = Bytes.toBytes("chunk");

  private HBaseDatabase db;

  public HObjectListTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public void get(Context options, final Set<ObjectListChunkKey> objects,
      final AsyncCallback<Collection<ObjectListChunk>> callback) {
    db.submit(new Runnable() {
      @Override
      public void run() {
        List<Get> gets = new ArrayList<Get>();
        for (ObjectListChunkKey key: objects) {
          gets.add(new Get(key.toBytes()).addFamily(OBJECT_LIST_FAMILY));
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

      Collection<ObjectListChunk> parseChunks(Result[] results) {
        List<ObjectListChunk> objects =
          new ArrayList<ObjectListChunk>(results.length);
        for (Result result: results) {
          assert(result != null);
          ObjectListChunkKey key =
            ObjectListChunkKey.fromBytes(result.getRow());
          byte[] value = result.getValue(OBJECT_LIST_FAMILY, CHUNK);
          assert(value != null);
          objects.add(ObjectListChunk.fromBytes(key, value));
        }
        return objects;
      }
    });
  }

  @Override
  public void put(ObjectListChunk listChunk, WriteBuffer buffer)
      throws DhtException {
    try {
      HWriteBuffer wb = (HWriteBuffer) buffer;
      wb.put(
        new Put(listChunk.getRowKey().toBytes())
          .add(OBJECT_LIST_FAMILY, CHUNK, listChunk.toBytes()));
    } catch (Throwable err) {
      throw new DhtException(err);
    }
  }
}

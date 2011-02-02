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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkFragments;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.ChunkPrefetch;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.PackChunk;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class HChunkTable implements ChunkTable {
  public static final byte[] CHUNKS_FAMILY = Bytes.toBytes("chunks");
  public static final byte[] CHUNK = Bytes.toBytes("chunk");
  public static final byte[] INDEX = Bytes.toBytes("index");
  public static final byte[] FRAGMENTS = Bytes.toBytes("fragments");
  public static final byte[] PREFETCH = Bytes.toBytes("prefetch");

  private final HBaseDatabase db;

  public HChunkTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public void get(Context options, final Set<ChunkKey> keys,
      final AsyncCallback<Collection<PackChunk.Members>> callback) {
    db.submit(new Runnable() {
      @Override
      public void run() {
        List<Get> gets = new ArrayList<Get>();
        for (ChunkKey key: keys) {
          gets.add(new Get(key.toBytes()).addFamily(CHUNKS_FAMILY));
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

      Collection<PackChunk.Members> parseChunks(Result[] results) 
          throws DhtException {
        Collection<PackChunk.Members> chunks = 
          new ArrayList<PackChunk.Members>(results.length);
        for (Result result: results) {
          assert(result != null);
          PackChunk.Members m = new PackChunk.Members();
          ChunkKey key = ChunkKey.fromBytes(result.getRow());
          m.setChunkKey(key);
          byte[] value = result.getValue(CHUNKS_FAMILY, CHUNK);
          if (value != null) {
            m.setChunkData(value);
          }
          value = result.getValue(CHUNKS_FAMILY, INDEX);
          if (value != null) {
            m.setChunkIndex(value);
          }
          value = result.getValue(CHUNKS_FAMILY, FRAGMENTS);
          if (value != null) {
            m.setFragments(ChunkFragments.fromBytes(key, value));
          }
          value = result.getValue(CHUNKS_FAMILY, PREFETCH);
          if (value != null) {
            m.setPrefetch(ChunkPrefetch.fromBytes(key, value));
          }
          chunks.add(m);
        }
        return chunks;
      }
    });
  }

  @Override
  public void put(PackChunk.Members chunk, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    Put put = new Put(chunk.getChunkKey().toBytes());
    if (chunk.getChunkData() != null) {
      put.add(CHUNKS_FAMILY, CHUNK, chunk.getChunkData());
    }
    if (chunk.getChunkIndex() != null) {
      put.add(CHUNKS_FAMILY, INDEX, chunk.getChunkIndex());
    }
    if (chunk.getFragments() != null) {
      put.add(CHUNKS_FAMILY, FRAGMENTS, chunk.getFragments().toBytes());
    }
    if (chunk.getPrefetch() != null) {
      put.add(CHUNKS_FAMILY, PREFETCH, chunk.getPrefetch().toBytes());
    }
    try {
      wb.put(put);
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }

  @Override
  public void remove(ChunkKey key, WriteBuffer buffer) throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      wb.delete(new Delete(key.toBytes()));
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }
}

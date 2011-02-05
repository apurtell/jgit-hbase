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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.PackChunk;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class HChunkTable implements ChunkTable {
  public static final byte[] CHUNKS_FAMILY = Bytes.toBytes("chunks");
  public static final byte[] CHUNK = Bytes.toBytes("chunk");
  public static final byte[] INDEX = Bytes.toBytes("index");
  public static final byte[] FRAGMENTS = Bytes.toBytes("fragments");

  private final HBaseDatabase db;

  public HChunkTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public void get(final Set<ChunkKey> keys,
      final AsyncCallback<Collection<PackChunk>> callback) {
    db.submit(new Runnable() {
      @Override
      public void run() {
        List<Get> gets = new ArrayList<Get>();
        for (ChunkKey key: keys) {
          gets.add(new Get(key.asByteArray()).addFamily(CHUNKS_FAMILY));
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

  protected Collection<PackChunk> parseChunks(Result[] results)
      throws DhtException {
    Collection<PackChunk> chunks = new ArrayList<PackChunk>();
    for (Result result: results) {
      if (result == null || result.getRow() == null) {
        continue;
      }
      ChunkKey key = ChunkKey.fromBytes(result.getRow());
      PackChunk.Builder chunk = new PackChunk.Builder();
      chunk.setChunkKey(key);
      byte[] value = result.getValue(CHUNKS_FAMILY, CHUNK);
      if (value != null) {
        chunk.setChunkData(value);
      }
      value = result.getValue(CHUNKS_FAMILY, INDEX);
      if (value != null) {
        chunk.setChunkIndex(value);
      }
      value = result.getValue(CHUNKS_FAMILY, FRAGMENTS);
      if (value != null) {
        chunk.setFragments(value);
      }
      chunks.add(chunk.build());
    }
    return chunks;
  }

  @Override
  public void putData(ChunkKey key, byte[] data, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      Put put = new Put(key.asByteArray());
      put.add(CHUNKS_FAMILY, CHUNK, data);
      wb.put(put);
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }

  @Override
  public void putIndex(ChunkKey key, byte[] index, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      Put put = new Put(key.asByteArray());
      put.add(CHUNKS_FAMILY, INDEX, index);
      wb.put(put);
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }

  @Override
  public void putFragments(ChunkKey key, byte[] fragments, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      Put put = new Put(key.asByteArray());
      put.add(CHUNKS_FAMILY, FRAGMENTS, fragments);
      wb.put(put);
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }
}

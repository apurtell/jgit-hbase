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
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.CachedPackInfo;
import org.eclipse.jgit.storage.dht.CachedPackKey;
import org.eclipse.jgit.storage.dht.ChunkInfo;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class HRepositoryTable implements RepositoryTable {
  public static final byte[] REPOSITORY_FAMILY = Bytes.toBytes("repository");
  public static final byte[] CHUNK_INFO = Bytes.toBytes("chunk-");
  public static final byte[] PACK_INFO = Bytes.toBytes("pack-");

  private final HBaseDatabase db;

  public HRepositoryTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public void put(RepositoryKey repo, ChunkInfo info, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      wb.put(new Put(repo.toBytes()).add(REPOSITORY_FAMILY,
        Bytes.add(CHUNK_INFO, info.getChunkKey().toShortBytes()),
        info.toBytes()));
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }

  @Override
  public void remove(RepositoryKey repo, ChunkKey chunk, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      wb.delete(new Delete(repo.toBytes()).deleteColumn(REPOSITORY_FAMILY,
        Bytes.add(CHUNK_INFO, chunk.toShortBytes())));
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }

  @Override
  public Collection<CachedPackInfo> getCachedPacks(RepositoryKey repo)
      throws DhtException, TimeoutException {
    Collection<CachedPackInfo> packs = new ArrayList<CachedPackInfo>();
    HTableInterface table = db.getTable();
    try {
      Result result = table.get(new Get(repo.toBytes())
        .addFamily(REPOSITORY_FAMILY));
      if (result != null && !result.isEmpty()) {
        for (KeyValue kv: result.raw()) {
          if (Bytes.startsWith(PACK_INFO, kv.getQualifier())) {
            packs.add(CachedPackInfo.fromBytes(repo, kv.getValue()));
          }
        }
      }
    } catch (IOException e) {
      throw new DhtException(e);
    } finally {
      db.putTable(table);
    }
    return packs;
  }

  @Override
  public void put(RepositoryKey repo, CachedPackInfo info, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      wb.put(new Put(repo.toBytes()).add(REPOSITORY_FAMILY,
        Bytes.add(PACK_INFO, info.getRowKey().toBytes()), info.toBytes()));
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }

  @Override
  public void remove(RepositoryKey repo, CachedPackKey key, WriteBuffer buffer)
      throws DhtException {
    HWriteBuffer wb = (HWriteBuffer) buffer;
    try {
      wb.delete(new Delete(repo.toBytes()).deleteColumn(REPOSITORY_FAMILY,
        Bytes.add(PACK_INFO, key.toBytes())));
    } catch (IOException e) {
      throw new DhtException(e);
    }
  }
}

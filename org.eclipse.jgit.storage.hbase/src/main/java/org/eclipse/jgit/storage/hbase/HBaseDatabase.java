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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.util.StringUtils;

import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Database;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class HBaseDatabase implements Database {
  public static final Log LOG = LogFactory.getLog(HBaseDatabase.class);

  private static final int POOL_SIZE = 10;

  private final Configuration conf;
  private final String tableName;
  private final HTablePool tablePool;
  private final ExecutorService executors;
  private final RepositoryIndexTable repositoryIndex;
  private final HRepositoryTable repositories;
  private final HRefTable refs;
  private final HObjectIndexTable objectIndex;
  private final HChunkTable chunks;

  public HBaseDatabase(Configuration conf, String tableName,
      ExecutorService executorService) throws IOException {
    this.conf = conf;
    this.tableName = tableName;
    this.executors = executorService;
    this.tablePool = new HTablePool(conf, POOL_SIZE);
    this.repositoryIndex = new HRepositoryIndexTable(this);
    this.repositories = new HRepositoryTable(this);
    this.refs = new HRefTable(this);
    this.objectIndex = new HObjectIndexTable(this);
    this.chunks = new HChunkTable(this);
  }

  HTableInterface getTable() {
    return tablePool.getTable(tableName);
  }

  void putTable(HTableInterface table) {
    tablePool.putTable(table);
  }

  @Override
  public RepositoryIndexTable repositoryIndex() {
    return repositoryIndex;
  }

  @Override
  public RepositoryTable repository() {
    return repositories;
  }

  @Override
  public RefTable ref() {
    return refs;
  }

  @Override
  public ObjectIndexTable objectIndex() {
    return objectIndex;
  }

  @Override
  public ChunkTable chunk() {
    return chunks;
  }

  @Override
  public WriteBuffer newWriteBuffer() {
    try {
      HWriteBuffer table = new HWriteBuffer(conf, tableName);
      table.setAutoFlush(false);
      table.setWriteBufferSize(1 * 1024 * 1024);
      return table;
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      return null;
    }
  }

  <T> Future<T> submit(Callable<T> task) {
    return executors.submit(task);
  }

  Future<?> submit(Runnable task) {
    return executors.submit(task);
  }
}

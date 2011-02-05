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
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;

public class HRepositoryTable implements RepositoryTable {
  public static final byte[] REPOSITORY_FAMILY = Bytes.toBytes("repository");
  public static final byte[] REF = Bytes.toBytes("ref");

  private final HBaseDatabase db;

  public HRepositoryTable(HBaseDatabase db) {
    this.db = db;
  }

  @Override
  public RepositoryKey get(RepositoryName name) throws DhtException,
      TimeoutException {
    HTableInterface table = db.getTable();
    try {
      Result result = table.get(new Get(name.asByteArray()));
      if (result == null) {
        return null;
      }
      byte[] value = result.getValue(REPOSITORY_FAMILY, REF);
      if (value == null) {
        return null;
      }
      return RepositoryKey.fromBytes(value);
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
      table.put(new Put(name.asByteArray()).add(REPOSITORY_FAMILY, REF,
        key.asByteArray()));
    } catch (IOException e) {
      throw new DhtException(e);
    } finally {
      db.putTable(table);
    }
  }
}

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

package org.eclipse.jgit.storage.hbase.pgm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.hbase.HBaseRepositoryBuilder;
import org.eclipse.jgit.storage.hbase.HChunkTable;
import org.eclipse.jgit.storage.hbase.HObjectIndexTable;
import org.eclipse.jgit.storage.hbase.HObjectListTable;
import org.eclipse.jgit.storage.hbase.HRefTable;
import org.eclipse.jgit.storage.hbase.HRepositoryIndexTable;
import org.eclipse.jgit.storage.hbase.HRepositoryTable;

import org.kohsuke.args4j.Argument;

@Command(common = true, name="hbase-init")
class HBaseInit extends TextBuiltin {
  @Argument(index = 0, required = true, metaVar = "git+hbase://")
  String uri;

  @Override
  protected boolean requiresRepository() {
    return false;
  }

  private void createTableIfMissing(HBaseRepositoryBuilder builder)
      throws IOException {
    Configuration conf = HBaseConfiguration.create();
    if (builder.getHost() != null) {
      conf.set("hbase.zookeeper.quorum", builder.getHost());
    }
    if (builder.getPort() != -1) {
      conf.setInt("hbase.zookeeper.property.clientPort", builder.getPort());
    }
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (!admin.tableExists(builder.getTableName())) {
      HTableDescriptor table = new HTableDescriptor(builder.getTableName());
      table.addFamily(new HColumnDescriptor(
          HChunkTable.CHUNKS_FAMILY,
          1, // maxVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          false, // blockCacheEnabled,
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER));
      table.addFamily(new HColumnDescriptor(
          HObjectIndexTable.OBJECT_INDEX_FAMILY,
          1, // maxVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          true, // blockCacheEnabled,
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER));
      table.addFamily(new HColumnDescriptor(
          HObjectListTable.OBJECT_LIST_FAMILY,
          1, // maxVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          true, // blockCacheEnabled,
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER));
      table.addFamily(new HColumnDescriptor(
          HRefTable.REF_FAMILY,
          1, // maxVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          true, // blockCacheEnabled,
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER));
      table.addFamily(new HColumnDescriptor(
          HRepositoryTable.REPOSITORY_FAMILY,
          1, // maxVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          true, // blockCacheEnabled,
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER));
      table.addFamily(new HColumnDescriptor(
          HRepositoryIndexTable.REPOSITORY_INDEX_FAMILY,
          1, // maxVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          true, // blockCacheEnabled,
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER));
      admin.createTable(table);
    }
  }

  @Override
  protected void run() throws Exception {
    int now = (int) (System.currentTimeMillis() / 1000L);
    RepositoryKey key = RepositoryKey.create(now);
    HBaseRepositoryBuilder builder = new HBaseRepositoryBuilder()
      .setURI(uri)
      .setRepositoryKey(key);
    createTableIfMissing(builder);
    builder.setDatabase(Main.connect(uri));
    builder.build().create(true);
    System.out.println("Created " + key + ":");
    System.out.println("  table: " + builder.getTableName());
    System.out.println("  repository: " + builder.getRepositoryName());
    System.exit(0);
  }
}

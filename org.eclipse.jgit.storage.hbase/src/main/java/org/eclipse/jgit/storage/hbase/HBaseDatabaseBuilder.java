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
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.eclipse.jgit.transport.URIish;

public class HBaseDatabaseBuilder {
  private String host;
  private int port = -1;
  private String tableName;
  private ExecutorService executorService;

  public HBaseDatabaseBuilder setURI(final String url)
      throws URISyntaxException {
    URIish u = new URIish(url);
    if (!"git+hbase".equals(u.getScheme())) {
      throw new IllegalArgumentException();
    }
    if (u.getHost() != null) {
      host = u.getHost();
      if (u.getPort() != -1) {
        port = u.getPort();
      }
    }
    String path = u.getPath();
    if (path.startsWith("/"))
      path = path.substring(1);
    int endTableName = path.indexOf('/');
    if (endTableName == -1) {
      endTableName = path.length();
    }
    setTableName(path.substring(0, endTableName));
    return this;
  }

  public String getHost() {
    return host;
  }

  public HBaseDatabaseBuilder setHost(String host) {
    this.host = host;
    return this;
  }

  public int getPort() {
    return port;
  }

  public HBaseDatabaseBuilder setPort(int port) {
    this.port = port;
    return this;
  }

  public String getTableName() {
    return tableName;
  }

  public HBaseDatabaseBuilder setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public HBaseDatabaseBuilder setExecutorService(
      ExecutorService executorService) {
    this.executorService = executorService;
    return this;
  }

  public HBaseDatabase build() throws IOException {
    if (executorService == null) {
      executorService = Executors.newCachedThreadPool();
    }
    Configuration conf = HBaseConfiguration.create();
    if (host != null) {
      conf.set("hbase.zookeeper.quorum", host);
    }
    if (port != -1) {
      conf.setInt("hbase.zookeeper.property.clientPort", port);
    }
    return new HBaseDatabase(conf, tableName, executorService);
  }
}

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

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.DhtRepository;
import org.eclipse.jgit.storage.dht.DhtRepositoryBuilder;
import org.eclipse.jgit.transport.URIish;

public class HBaseRepositoryBuilder
    extends DhtRepositoryBuilder<HBaseRepositoryBuilder, DhtRepository, HBaseDatabase> {
  private static ExecutorService defaultExecutorService;

  private ExecutorService executorService;
  private String host = null;
  private int port = -1;
  private String tableName;
  
  public static synchronized ExecutorService getDefaultExecutorService() {
    if (defaultExecutorService == null) {
      final int ncpu = Runtime.getRuntime().availableProcessors();
      defaultExecutorService = Executors.newFixedThreadPool(2 * ncpu);
    }
    return defaultExecutorService;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public HBaseRepositoryBuilder setExecutorService(
      ExecutorService executorService) {
    this.executorService = executorService;
    return self();
  }

  public HBaseRepositoryBuilder setURI(final String url)
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
    setTableName(path.substring(0, endTableName));
    setRepositoryName(path.substring(endTableName + 1));
    return this;
  }

  public String getHost() {
    return host;
  }

  public HBaseRepositoryBuilder setHost(String host) {
    this.host = host;
    return this;
  }

  public int getPort() {
    return port;
  }

  public HBaseRepositoryBuilder setPort(int port) {
    this.port = port;
    return this;
  }

  public String getTableName() {
    return tableName;
  }

  public HBaseRepositoryBuilder setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  @Override
  public HBaseRepositoryBuilder setup() throws IllegalArgumentException,
      DhtException, RepositoryNotFoundException {
    if (getDatabase() == null) {
      try {
        setDatabase(new HBaseDatabaseBuilder()
        .setHost(host)
        .setPort(port)
        .setTableName(tableName)
        .setExecutorService(getExecutorService())
        .build());
      } catch (IOException e) {
        throw new DhtException(e);
      }
    }
    return super.setup();
  }

  @Override
  public DhtRepository build() throws IllegalArgumentException, DhtException,
      RepositoryNotFoundException {
    return new DhtRepository(setup());
  }
}

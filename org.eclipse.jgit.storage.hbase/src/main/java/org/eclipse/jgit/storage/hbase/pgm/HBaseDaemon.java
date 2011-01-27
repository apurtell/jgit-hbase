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

import java.io.File;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;


import org.eclipse.jgit.pgm.CLIText;
import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.storage.hbase.HBaseDatabase;
import org.eclipse.jgit.storage.hbase.HBaseDatabaseBuilder;
import org.eclipse.jgit.storage.hbase.HBaseRepositoryBuilder;
import org.eclipse.jgit.storage.pack.PackConfig;
import org.eclipse.jgit.transport.Daemon;
import org.eclipse.jgit.transport.DaemonService;
import org.eclipse.jgit.util.FS;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

@Command(common = true, name="hbase-daemon")
class HBaseDaemon extends TextBuiltin {
  @Option(name = "--config-file", metaVar = "metaVar_configFile", usage = "usage_configFile")
  File configFile;

  @Option(name = "--port", metaVar = "metaVar_port", usage = "usage_portNumberToListenOn")
  int port = org.eclipse.jgit.transport.Daemon.DEFAULT_PORT;

  @Option(name = "--listen", metaVar = "metaVar_hostName", usage = "usage_hostnameOrIpToListenOn")
  String host;

  @Option(name = "--timeout", metaVar = "metaVar_seconds", usage = "usage_abortConnectionIfNoActivity")
  int timeout = -1;

  @Option(name = "--enable", metaVar = "metaVar_service", usage = "usage_enableTheServiceInAllRepositories", multiValued = true)
  final List<String> enable = new ArrayList<String>();

  @Option(name = "--disable", metaVar = "metaVar_service", usage = "usage_disableTheServiceInAllRepositories", multiValued = true)
  final List<String> disable = new ArrayList<String>();

  @Option(name = "--allow-override", metaVar = "metaVar_service", usage = "usage_configureTheServiceInDaemonServicename", multiValued = true)
  final List<String> canOverride = new ArrayList<String>();

  @Option(name = "--forbid-override", metaVar = "metaVar_service", usage = "usage_configureTheServiceInDaemonServicename", multiValued = true)
  final List<String> forbidOverride = new ArrayList<String>();

  @Argument(index = 0, required = true, metaVar = "git+hbase://")
  String uri;

  @Argument(index = 1, required = true, metaVar = "metaVar_directory", usage = "usage_directoriesToExport")
  final List<String> names = new ArrayList<String>();

  @Override
  protected boolean requiresRepository() {
    return false;
  }

  @Override
  protected void run() throws Exception {
    PackConfig packConfig = new PackConfig();

    // TODO Temporary until getSize() is correctly implemented.
    packConfig.setDeltaCompress(false);

    if (configFile != null) {
      if (!configFile.exists()) {
        throw die(MessageFormat.format(
            CLIText.get().configFileNotFound, //
            configFile.getAbsolutePath()));
      }

      FileBasedConfig cfg = new FileBasedConfig(configFile, FS.DETECTED);
      cfg.load();
      packConfig.fromConfig(cfg);
    }

    int threads = packConfig.getThreads();
    if (threads <= 0) {
      threads = Runtime.getRuntime().availableProcessors();
    }
    if (1 < threads) {
      packConfig.setExecutor(Executors.newFixedThreadPool(threads));
    }

    final Daemon daemon = new Daemon(
      host != null ? new InetSocketAddress(host, port)
        : new InetSocketAddress(port));

    HBaseDatabase db = new HBaseDatabaseBuilder().setURI(uri).build();

    for (String name: names) {  
      daemon.exportRepository(name,
        new HBaseRepositoryBuilder()
          .setDatabase(db)
          .setRepositoryName(name).build());
    }

    daemon.setPackConfig(packConfig);
    if (0 <= timeout) {
      daemon.setTimeout(timeout);
    }
    for (final String n: enable) {
      service(daemon, n).setEnabled(true);
    }
    for (final String n: disable) {
      service(daemon, n).setEnabled(false);
    }
    for (final String n: canOverride) {
      service(daemon, n).setOverridable(true);
    }
    for (final String n: forbidOverride) {
      service(daemon, n).setOverridable(false);
    }
    daemon.start();
    out.println(MessageFormat.format(CLIText.get().listeningOn,
      daemon.getAddress()));
  }

  private DaemonService service(final Daemon daemon, final String n) {
    final DaemonService svc = daemon.getService(n);
    if (svc == null) {
      throw die(MessageFormat.format(CLIText.get().serviceNotSupported, n));
    }
    return svc;
  }
}

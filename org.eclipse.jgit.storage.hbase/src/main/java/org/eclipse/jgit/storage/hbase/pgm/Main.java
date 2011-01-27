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
import java.net.URISyntaxException;


import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.pgm.Die;
import org.eclipse.jgit.storage.hbase.HBaseDatabase;
import org.eclipse.jgit.storage.hbase.HBaseDatabaseBuilder;
import org.eclipse.jgit.storage.hbase.HBaseRepositoryBuilder;

public class Main extends org.eclipse.jgit.pgm.Main {
  public static void main(final String[] argv) {
    new Main().run(argv);
  }

  protected static HBaseDatabase connect(String uri)
      throws IOException, URISyntaxException {
    HBaseDatabaseBuilder builder = new HBaseDatabaseBuilder().setURI(uri);
    return builder.build();
  }

  @Override
  protected Repository openGitDir(String gitdir) throws IOException {
    if (gitdir != null && gitdir.startsWith("git+hbase://")) {
      try {
        return new HBaseRepositoryBuilder().setURI(gitdir).build();
      } catch (URISyntaxException e) {
        throw new Die("Invalid URI " + gitdir);
      }
    }
    return super.openGitDir(gitdir);
  }
}

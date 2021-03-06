/*
 * Copyright (C) 2011, Google Inc.
 * and other copyright owners as documented in the project's IP log.
 *
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Distribution License v1.0 which
 * accompanies this distribution, is reproduced below, and is
 * available at http://www.eclipse.org/org/documents/edl-v10.php
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 *
 * - Neither the name of the Eclipse Foundation, Inc. nor the
 *   names of its contributors may be used to endorse or promote
 *   products derived from this software without specific prior
 *   written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.eclipse.jgit.storage.hbase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.spi.util.ExecutorTools;
import org.eclipse.jgit.transport.URIish;

/** Constructs a {@link HBaseDatabase} instance. */
public class HBaseDatabaseBuilder {
	private String hosts;

	private String schemaPrefix;

	private Configuration configuration;

	private ExecutorService executorService;

	public HBaseDatabaseBuilder setURI(final String url)
			throws URISyntaxException {
		URIish u = new URIish(url);
		if (!"git+hbase".equals(u.getScheme()))
			throw new IllegalArgumentException();

		String host = u.getHost();
		if (host != null)
			setHosts(host);

		String path = u.getPath();
		if (path.startsWith("/"))
			path = path.substring(1);

		int endPrefix = path.indexOf('/');
		if (endPrefix < 0)
			endPrefix = path.length();
		setSchemaPrefix(path.substring(0, endPrefix));
		return this;
	}

	public String getSchemaPrefix() {
		return schemaPrefix;
	}

	public HBaseDatabaseBuilder setSchemaPrefix(String schemaPrefix) {
		if (schemaPrefix != null && 0 < schemaPrefix.length())
			this.schemaPrefix = schemaPrefix;
		else
			this.schemaPrefix = null;
		return this;
	}

	public String getHosts() {
		return hosts;
	}

	public HBaseDatabaseBuilder setHosts(String hosts) {
		this.hosts = hosts;
		this.configuration = null;
		return this;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public HBaseDatabaseBuilder setConfiguration(Configuration conf) {
		this.configuration = conf;
		return this;
	}

	public ExecutorService getExecutorService() {
		return executorService;
	}

	public HBaseDatabaseBuilder setExecutorService(
			ExecutorService executorService) {
		this.executorService = executorService;
		return this;
	}

	public HBaseDatabaseBuilder setup() {
		if (configuration == null) {
			configuration = HBaseConfiguration.create();
			if (getHosts() != null){
				configuration.set("hbase.zookeeper.quorum", getHosts());
				configuration.setInt("hbase.zookeeper.property.clientPort", 2181);
			}
		}

		if (executorService == null)
			executorService = ExecutorTools.getDefaultExecutorService();

		return this;
	}

	/**
	 * @return create and return the database connection.
	 * @throws IOException
	 *             the connection cannot be setup.
	 */
	public HBaseDatabase build() throws DhtException {
		return new HBaseDatabase(setup());
	}
}

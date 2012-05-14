/*
 * Copyright (C) 2010, Google Inc.
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

package org.eclipse.jgit.storage.hbase.pgm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.eclipse.jgit.storage.hbase.HBaseDatabaseBuilder;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

@Command(name = "hbase-create-schema")
class HBaseCreateSchema extends TextBuiltin {
	@Option(name = "--compression", required = false, metaVar = "metaVar_compression")
	String compression;

	@Argument(index = 0, required = true, metaVar = "git+hbase://")
	String uri;

	private HBaseAdmin admin;

	private Configuration config;

	private String schemaPrefix;

	@Override
	protected boolean requiresRepository() {
		return false;
	}

	@Override
	protected void run() throws Exception {
		HBaseDatabaseBuilder builder = new HBaseDatabaseBuilder().setURI(uri)
				.setup();

		config = builder.getConfiguration();
		schemaPrefix = builder.getSchemaPrefix();
		admin = new HBaseAdmin(config);

		createSequence();
		createRepositoryIndex();
		createRepository();
		createRef();
		createObjectIndex();
		createChunkInfo();
		createChunk();

		HConnectionManager.deleteAllConnections(true);
	}

	private void createSequence() throws IOException {
		HTableDescriptor table = table("SEQUENCE");
		HColumnDescriptor col;

		col = column("next");
		noCompression(col);
		table.addFamily(col);

		admin.createTable(table);
	}

	private void createRepositoryIndex() throws IOException {
		HTableDescriptor table = table("REPOSITORY_INDEX");
		HColumnDescriptor col;

		col = column("id");
		table.addFamily(col);

		admin.createTable(table);
	}

	private void createRepository() throws IOException {
		HTableDescriptor table = table("REPOSITORY");
		HColumnDescriptor col;

		col = column("name");
		table.addFamily(col);

		col = column("cached-pack");
		table.addFamily(col);

		admin.createTable(table);
	}

	private void createRef() throws IOException {
		HTableDescriptor table = table("REF");
		HColumnDescriptor col;

		col = column("target");
		col.setMaxVersions(5);
		table.addFamily(col);

		admin.createTable(table);
	}

	private void createObjectIndex() throws IOException {
		HTableDescriptor table = table("OBJECT_INDEX");
		HColumnDescriptor col;

		col = column("info");
		col.setBloomFilterType(BloomType.ROW);
		table.addFamily(col);

		admin.createTable(table);
	}

	private void createChunkInfo() throws IOException {
		HTableDescriptor table = table("CHUNK_INFO");
		HColumnDescriptor col;

		col = column("chunk-info");
		table.addFamily(col);

		admin.createTable(table);
	}

	private void createChunk() throws IOException {
		HTableDescriptor table = table("CHUNK");
		HColumnDescriptor col;

		col = column("chunk");
		noCompression(col);
		table.addFamily(col);

		col = column("index");
		noCompression(col);
		table.addFamily(col);

		col = column("meta");
		table.addFamily(col);

		admin.createTable(table);
	}

	private HTableDescriptor table(String tableName) {
		if (schemaPrefix != null && schemaPrefix.length() > 0)
			tableName = schemaPrefix + "." + tableName;
		return new HTableDescriptor(tableName);
	}

	private HColumnDescriptor column(String familyName) {
		HColumnDescriptor col = new HColumnDescriptor(familyName);
		col.setMaxVersions(1);
		col.setBloomFilterType(BloomType.NONE);
		if (compression != null) {
			col.setCompressionType(Algorithm.valueOf(compression));
			col.setCompactionCompressionType(Algorithm.valueOf(compression));
		}
		return col;
	}

	private void noCompression(HColumnDescriptor col) {
		col.setCompressionType(Algorithm.NONE);
		col.setCompactionCompressionType(Algorithm.NONE);
	}
}

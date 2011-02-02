/*
 * Copyright (C) 2008-2011, Google Inc.
 * Copyright (C) 2007-2008, Robin Rosenberg <robin.rosenberg@dewire.com>
 * Copyright (C) 2008, Shawn O. Pearce <spearce@spearce.org>
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

package org.eclipse.jgit.transport;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.eclipse.jgit.JGitText;
import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.InflaterCache;
import org.eclipse.jgit.lib.MutableObjectId;
import org.eclipse.jgit.lib.NullProgressMonitor;
import org.eclipse.jgit.lib.ObjectChecker;
import org.eclipse.jgit.lib.ObjectDatabase;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdSubclassMap;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.ObjectStream;
import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.storage.file.PackLock;
import org.eclipse.jgit.storage.pack.BinaryDelta;
import org.eclipse.jgit.util.IO;
import org.eclipse.jgit.util.NB;

/**
 * Parses a pack stream and imports it for an {@link ObjectInserter}.
 * <p>
 * Applications can acquire an instance of a parser from ObjectInserter's
 * {@link ObjectInserter#newPackParser(InputStream)} method.
 * <p>
 * Implementations of {@link ObjectInserter} should subclass this type and
 * provide their own logic for the various {@code on*()} event methods declared
 * to be abstract.
 */
public abstract class PackParser {
	/** Size of the internal stream buffer. */
	private static final int BUFFER_SIZE = 8192;

	/** Location data is being obtained from. */
	public static enum Source {
		/** Data is read from the incoming stream. */
		INPUT,

		/** Data is read back from the database's buffers. */
		DATABASE;
	}

	/** Object database used for loading existing objects. */
	private final ObjectDatabase objectDatabase;

	private InflaterStream inflater;

	private byte[] tempBuffer;

	private byte[] hdrBuf;

	private final MessageDigest objectDigest;

	private final MutableObjectId tempObjectId;

	private InputStream in;

	private byte[] buf;

	/** Position in the input stream of {@code buf[0]}. */
	private long bBase;

	private int bOffset;

	private int bAvail;

	private ObjectChecker objCheck;

	private boolean allowThin;

	private boolean checkObjectCollisions;

	private boolean needBaseObjectIds;

	private long objectCount;

	private PackedObjectInfo[] entries;

	/**
	 * Every object contained within the incoming pack.
	 * <p>
	 * This is a subset of {@link #entries}, as thin packs can add additional
	 * objects to {@code entries} by copying already existing objects from the
	 * repository onto the end of the thin pack to make it self-contained.
	 */
	private ObjectIdSubclassMap<ObjectId> newObjectIds;

	private int deltaCount;

	private int entryCount;

	private ObjectIdSubclassMap<DeltaChain> baseById;

	/**
	 * Objects referenced by their name from deltas, that aren't in this pack.
	 * <p>
	 * This is the set of objects that were copied onto the end of this pack to
	 * make it complete. These objects were not transmitted by the remote peer,
	 * but instead were assumed to already exist in the local repository.
	 */
	private ObjectIdSubclassMap<ObjectId> baseObjectIds;

	private LongMap<UnresolvedDelta> baseByPos;

	/** Blobs whose contents need to be double-checked after indexing. */
	private List<PackedObjectInfo> deferredCheckBlobs;

	private MessageDigest packDigest;

	private ObjectReader readCurs;

	/** Message to protect the pack data from garbage collection. */
	private String lockMessage;

	/**
	 * Initialize a pack parser.
	 *
	 * @param odb
	 *            database the parser will write its objects into.
	 * @param src
	 *            the stream the parser will read.
	 */
	protected PackParser(final ObjectDatabase odb, final InputStream src) {
		objectDatabase = odb.newCachedDatabase();
		in = src;

		inflater = new InflaterStream();
		readCurs = objectDatabase.newReader();
		buf = new byte[BUFFER_SIZE];
		tempBuffer = new byte[BUFFER_SIZE];
		hdrBuf = new byte[64];
		objectDigest = Constants.newMessageDigest();
		tempObjectId = new MutableObjectId();
		packDigest = Constants.newMessageDigest();
		checkObjectCollisions = true;
	}

	/** @return true if a thin pack (missing base objects) is permitted. */
	public boolean isAllowThin() {
		return allowThin;
	}

	/**
	 * Configure this index pack instance to allow a thin pack.
	 * <p>
	 * Thin packs are sometimes used during network transfers to allow a delta
	 * to be sent without a base object. Such packs are not permitted on disk.
	 *
	 * @param allow
	 *            true to enable a thin pack.
	 */
	public void setAllowThin(final boolean allow) {
		allowThin = allow;
	}

	/** @return if true received objects are verified to prevent collisions. */
	public boolean isCheckObjectCollisions() {
		return checkObjectCollisions;
	}

	/**
	 * Enable checking for collisions with existing objects.
	 * <p>
	 * By default PackParser looks for each received object in the repository.
	 * If the object already exists, the existing object is compared
	 * byte-for-byte with the newly received copy to ensure they are identical.
	 * The receive is aborted with an exception if any byte differs. This check
	 * is necessary to prevent an evil attacker from supplying a replacement
	 * object into this repository in the event that a discovery enabling SHA-1
	 * collisions is made.
	 * <p>
	 * This check may be very costly to perform, and some repositories may have
	 * other ways to segregate newly received object data. The check is enabled
	 * by default, but can be explicitly disabled if the implementation can
	 * provide the same guarantee, or is willing to accept the risks associated
	 * with bypassing the check.
	 *
	 * @param check
	 *            true to enable collision checking (strongly encouraged).
	 */
	public void setCheckObjectCollisions(boolean check) {
		checkObjectCollisions = check;
	}

	/**
	 * Configure this index pack instance to keep track of new objects.
	 * <p>
	 * By default an index pack doesn't save the new objects that were created
	 * when it was instantiated. Setting this flag to {@code true} allows the
	 * caller to use {@link #getNewObjectIds()} to retrieve that list.
	 *
	 * @param b
	 *            {@code true} to enable keeping track of new objects.
	 */
	public void setNeedNewObjectIds(boolean b) {
		if (b)
			newObjectIds = new ObjectIdSubclassMap<ObjectId>();
		else
			newObjectIds = null;
	}

	private boolean needNewObjectIds() {
		return newObjectIds != null;
	}

	/**
	 * Configure this index pack instance to keep track of the objects assumed
	 * for delta bases.
	 * <p>
	 * By default an index pack doesn't save the objects that were used as delta
	 * bases. Setting this flag to {@code true} will allow the caller to use
	 * {@link #getBaseObjectIds()} to retrieve that list.
	 *
	 * @param b
	 *            {@code true} to enable keeping track of delta bases.
	 */
	public void setNeedBaseObjectIds(boolean b) {
		this.needBaseObjectIds = b;
	}

	/** @return the new objects that were sent by the user */
	public ObjectIdSubclassMap<ObjectId> getNewObjectIds() {
		if (newObjectIds != null)
			return newObjectIds;
		return new ObjectIdSubclassMap<ObjectId>();
	}

	/** @return set of objects the incoming pack assumed for delta purposes */
	public ObjectIdSubclassMap<ObjectId> getBaseObjectIds() {
		if (baseObjectIds != null)
			return baseObjectIds;
		return new ObjectIdSubclassMap<ObjectId>();
	}

	/**
	 * Configure the checker used to validate received objects.
	 * <p>
	 * Usually object checking isn't necessary, as Git implementations only
	 * create valid objects in pack files. However, additional checking may be
	 * useful if processing data from an untrusted source.
	 *
	 * @param oc
	 *            the checker instance; null to disable object checking.
	 */
	public void setObjectChecker(final ObjectChecker oc) {
		objCheck = oc;
	}

	/**
	 * Configure the checker used to validate received objects.
	 * <p>
	 * Usually object checking isn't necessary, as Git implementations only
	 * create valid objects in pack files. However, additional checking may be
	 * useful if processing data from an untrusted source.
	 * <p>
	 * This is shorthand for:
	 *
	 * <pre>
	 * setObjectChecker(on ? new ObjectChecker() : null);
	 * </pre>
	 *
	 * @param on
	 *            true to enable the default checker; false to disable it.
	 */
	public void setObjectChecking(final boolean on) {
		setObjectChecker(on ? new ObjectChecker() : null);
	}

	/** @return the message to record with the pack lock. */
	public String getLockMessage() {
		return lockMessage;
	}

	/**
	 * Set the lock message for the incoming pack data.
	 *
	 * @param msg
	 *            if not null, the message to associate with the incoming data
	 *            while it is locked to prevent garbage collection.
	 */
	public void setLockMessage(String msg) {
		lockMessage = msg;
	}

	/**
	 * Get the number of objects in the stream.
	 * <p>
	 * The object count is only available after {@link #parse(ProgressMonitor)}
	 * has returned. The count may have been increased if the stream was a thin
	 * pack, and missing bases objects were appending onto it by the subclass.
	 *
	 * @return number of objects parsed out of the stream.
	 */
	public int getObjectCount() {
		return entryCount;
	}

	/***
	 * Get the information about the requested object.
	 * <p>
	 * The object information is only available after
	 * {@link #parse(ProgressMonitor)} has returned.
	 *
	 * @param nth
	 *            index of the object in the stream. Must be between 0 and
	 *            {@link #getObjectCount()}-1.
	 * @return the object information.
	 */
	public PackedObjectInfo getObject(int nth) {
		return entries[nth];
	}

	/**
	 * Get all of the objects, sorted by their name.
	 * <p>
	 * The object information is only available after
	 * {@link #parse(ProgressMonitor)} has returned.
	 * <p>
	 * To maintain lower memory usage and good runtime performance, this method
	 * sorts the objects in-place and therefore impacts the ordering presented
	 * by {@link #getObject(int)}.
	 *
	 * @param cmp
	 *            comparison function, if null objects are stored by ObjectId.
	 * @return sorted list of objects in this pack stream.
	 */
	public List<PackedObjectInfo> getSortedObjectList(
			Comparator<PackedObjectInfo> cmp) {
		Arrays.sort(entries, 0, entryCount, cmp);
		List<PackedObjectInfo> list = Arrays.asList(entries);
		if (entryCount < entries.length)
			list = list.subList(0, entryCount);
		return list;
	}

	/**
	 * Parse the pack stream.
	 *
	 * @param progress
	 *            callback to provide progress feedback during parsing. If null,
	 *            {@link NullProgressMonitor} will be used.
	 * @return the pack lock, if one was requested by setting
	 *         {@link #setLockMessage(String)}.
	 * @throws IOException
	 *             the stream is malformed, or contains corrupt objects.
	 */
	public final PackLock parse(ProgressMonitor progress) throws IOException {
		return parse(progress, progress);
	}

	/**
	 * Parse the pack stream.
	 *
	 * @param receiving
	 *            receives progress feedback during the initial receiving
	 *            objects phase. If null, {@link NullProgressMonitor} will be
	 *            used.
	 * @param resolving
	 *            receives progress feedback during the resolving objects phase.
	 * @return the pack lock, if one was requested by setting
	 *         {@link #setLockMessage(String)}.
	 * @throws IOException
	 *             the stream is malformed, or contains corrupt objects.
	 */
	public PackLock parse(ProgressMonitor receiving, ProgressMonitor resolving)
			throws IOException {
		if (receiving == null)
			receiving = NullProgressMonitor.INSTANCE;
		if (resolving == null)
			resolving = NullProgressMonitor.INSTANCE;

		if (receiving == resolving)
			receiving.start(2 /* tasks */);
		try {
			readPackHeader();

			entries = new PackedObjectInfo[(int) objectCount];
			baseById = new ObjectIdSubclassMap<DeltaChain>();
			baseByPos = new LongMap<UnresolvedDelta>();
			deferredCheckBlobs = new ArrayList<PackedObjectInfo>();

			receiving.beginTask(JGitText.get().receivingObjects,
					(int) objectCount);
			try {
				for (int done = 0; done < objectCount; done++) {
					indexOneObject();
					receiving.update(1);
					if (receiving.isCancelled())
						throw new IOException(JGitText.get().downloadCancelled);
				}
				readPackFooter();
				endInput();
			} finally {
				receiving.endTask();
			}

			if (!deferredCheckBlobs.isEmpty())
				doDeferredCheckBlobs();
			if (deltaCount > 0) {
				resolveDeltas(resolving);
				if (entryCount < objectCount) {
					if (!isAllowThin()) {
						throw new IOException(MessageFormat.format(JGitText
								.get().packHasUnresolvedDeltas,
								(objectCount - entryCount)));
					}

					resolveDeltasWithExternalBases(resolving);

					if (entryCount < objectCount) {
						throw new IOException(MessageFormat.format(JGitText
								.get().packHasUnresolvedDeltas,
								(objectCount - entryCount)));
					}
				}
			}

			packDigest = null;
			baseById = null;
			baseByPos = null;
		} finally {
			try {
				if (readCurs != null)
					readCurs.release();
			} finally {
				readCurs = null;
			}

			try {
				inflater.release();
			} finally {
				inflater = null;
				objectDatabase.close();
			}
		}
		return null; // By default there is no locking.
	}

	private void resolveDeltas(final ProgressMonitor progress)
			throws IOException {
		progress.beginTask(JGitText.get().resolvingDeltas, deltaCount);
		final int last = entryCount;
		for (int i = 0; i < last; i++) {
			final int before = entryCount;
			resolveDeltas(entries[i]);
			progress.update(entryCount - before);
			if (progress.isCancelled())
				throw new IOException(
						JGitText.get().downloadCancelledDuringIndexing);
		}
		progress.endTask();
	}

	private void resolveDeltas(final PackedObjectInfo oe) throws IOException {
		UnresolvedDelta children = firstChildOf(oe);
		if (children == null)
			return;

		DeltaVisit visit = new DeltaVisit();
		visit.nextChild = children;

		ObjectTypeAndSize info = openDatabase(oe, new ObjectTypeAndSize());
		switch (info.type) {
		case Constants.OBJ_COMMIT:
		case Constants.OBJ_TREE:
		case Constants.OBJ_BLOB:
		case Constants.OBJ_TAG:
			visit.data = inflateAndReturn(Source.DATABASE, info.size);
			visit.id = oe;
			break;
		default:
			throw new IOException(MessageFormat.format(
					JGitText.get().unknownObjectType, info.type));
		}

		if (!checkCRC(oe.getCRC())) {
			throw new IOException(MessageFormat.format(
					JGitText.get().corruptionDetectedReReadingAt, oe
							.getOffset()));
		}

		resolveDeltas(visit.next(), info.type, info);
	}

	private void resolveDeltas(DeltaVisit visit, final int type,
			ObjectTypeAndSize info) throws IOException {
		do {
			info = openDatabase(visit.delta, info);
			switch (info.type) {
			case Constants.OBJ_OFS_DELTA:
			case Constants.OBJ_REF_DELTA:
				break;

			default:
				throw new IOException(MessageFormat.format(
						JGitText.get().unknownObjectType, info.type));
			}

			visit.data = BinaryDelta.apply(visit.parent.data, //
					inflateAndReturn(Source.DATABASE, info.size));

			if (!checkCRC(visit.delta.crc))
				throw new IOException(MessageFormat.format(
						JGitText.get().corruptionDetectedReReadingAt,
						visit.delta.position));

			objectDigest.update(Constants.encodedTypeString(type));
			objectDigest.update((byte) ' ');
			objectDigest.update(Constants.encodeASCII(visit.data.length));
			objectDigest.update((byte) 0);
			objectDigest.update(visit.data);
			tempObjectId.fromRaw(objectDigest.digest(), 0);

			verifySafeObject(tempObjectId, type, visit.data);

			PackedObjectInfo oe;
			oe = newInfo(tempObjectId, visit.delta, visit.parent.id);
			oe.setOffset(visit.delta.position);
			addObjectAndTrack(oe);
			visit.id = oe;

			visit.nextChild = firstChildOf(oe);
			visit = visit.next();
		} while (visit != null);
	}

	/**
	 * Read the header of the current object.
	 * <p>
	 * After the header has been parsed, this method automatically invokes
	 * {@link #onObjectHeader(Source, byte[], int, int)} to allow the
	 * implementation to update its internal checksums for the bytes read.
	 * <p>
	 * When this method returns the database will be positioned on the first
	 * byte of the deflated data stream.
	 *
	 * @param info
	 *            the info object to populate.
	 * @return {@code info}, after populating.
	 * @throws IOException
	 *             the size cannot be read.
	 */
	protected ObjectTypeAndSize readObjectHeader(ObjectTypeAndSize info)
			throws IOException {
		int hdrPtr = 0;
		int c = readFrom(Source.DATABASE);
		hdrBuf[hdrPtr++] = (byte) c;

		info.type = (c >> 4) & 7;
		long sz = c & 15;
		int shift = 4;
		while ((c & 0x80) != 0) {
			c = readFrom(Source.DATABASE);
			hdrBuf[hdrPtr++] = (byte) c;
			sz += (c & 0x7f) << shift;
			shift += 7;
		}
		info.size = sz;

		switch (info.type) {
		case Constants.OBJ_COMMIT:
		case Constants.OBJ_TREE:
		case Constants.OBJ_BLOB:
		case Constants.OBJ_TAG:
			onObjectHeader(Source.DATABASE, hdrBuf, 0, hdrPtr);
			break;

		case Constants.OBJ_OFS_DELTA:
			c = readFrom(Source.DATABASE);
			hdrBuf[hdrPtr++] = (byte) c;
			while ((c & 128) != 0) {
				c = readFrom(Source.DATABASE);
				hdrBuf[hdrPtr++] = (byte) c;
			}
			onObjectHeader(Source.DATABASE, hdrBuf, 0, hdrPtr);
			break;

		case Constants.OBJ_REF_DELTA:
			System.arraycopy(buf, fill(Source.DATABASE, 20), hdrBuf, hdrPtr, 20);
			hdrPtr += 20;
			use(20);
			onObjectHeader(Source.DATABASE, hdrBuf, 0, hdrPtr);
			break;

		default:
			throw new IOException(MessageFormat.format(
					JGitText.get().unknownObjectType, info.type));
		}
		return info;
	}

	private UnresolvedDelta removeBaseById(final AnyObjectId id) {
		final DeltaChain d = baseById.get(id);
		return d != null ? d.remove() : null;
	}

	private static UnresolvedDelta reverse(UnresolvedDelta c) {
		UnresolvedDelta tail = null;
		while (c != null) {
			final UnresolvedDelta n = c.next;
			c.next = tail;
			tail = c;
			c = n;
		}
		return tail;
	}

	private UnresolvedDelta firstChildOf(PackedObjectInfo oe) {
		UnresolvedDelta a = reverse(removeBaseById(oe));
		UnresolvedDelta b = reverse(baseByPos.remove(oe.getOffset()));

		if (a == null)
			return b;
		if (b == null)
			return a;

		UnresolvedDelta first = null;
		UnresolvedDelta last = null;
		while (a != null || b != null) {
			UnresolvedDelta curr;
			if (b == null || (a != null && a.position < b.position)) {
				curr = a;
				a = a.next;
			} else {
				curr = b;
				b = b.next;
			}
			if (last != null)
				last.next = curr;
			else
				first = curr;
			last = curr;
			curr.next = null;
		}
		return first;
	}

	private void resolveDeltasWithExternalBases(final ProgressMonitor progress)
			throws IOException {
		growEntries(baseById.size());

		if (needBaseObjectIds)
			baseObjectIds = new ObjectIdSubclassMap<ObjectId>();

		final List<DeltaChain> missing = new ArrayList<DeltaChain>(64);
		for (final DeltaChain baseId : baseById) {
			if (baseId.head == null)
				continue;

			if (needBaseObjectIds)
				baseObjectIds.add(baseId);

			final ObjectLoader ldr;
			try {
				ldr = readCurs.open(baseId);
			} catch (MissingObjectException notFound) {
				missing.add(baseId);
				continue;
			}

			final DeltaVisit visit = new DeltaVisit();
			visit.data = ldr.getCachedBytes(Integer.MAX_VALUE);
			visit.id = baseId;
			final int typeCode = ldr.getType();
			final PackedObjectInfo oe = newInfo(baseId, null, null);

			if (onAppendBase(typeCode, visit.data, oe))
				entries[entryCount++] = oe;

			visit.nextChild = firstChildOf(oe);
			resolveDeltas(visit.next(), typeCode, new ObjectTypeAndSize());

			if (progress.isCancelled())
				throw new IOException(
						JGitText.get().downloadCancelledDuringIndexing);
		}

		for (final DeltaChain base : missing) {
			if (base.head != null)
				throw new MissingObjectException(base, "delta base");
		}

		onEndThinPack();
	}

	private void growEntries(int extraObjects) {
		final PackedObjectInfo[] ne;

		ne = new PackedObjectInfo[(int) objectCount + extraObjects];
		System.arraycopy(entries, 0, ne, 0, entryCount);
		entries = ne;
	}

	private void readPackHeader() throws IOException {
		final int hdrln = Constants.PACK_SIGNATURE.length + 4 + 4;
		final int p = fill(Source.INPUT, hdrln);
		for (int k = 0; k < Constants.PACK_SIGNATURE.length; k++)
			if (buf[p + k] != Constants.PACK_SIGNATURE[k])
				throw new IOException(JGitText.get().notAPACKFile);

		final long vers = NB.decodeUInt32(buf, p + 4);
		if (vers != 2 && vers != 3)
			throw new IOException(MessageFormat.format(
					JGitText.get().unsupportedPackVersion, vers));
		objectCount = NB.decodeUInt32(buf, p + 8);
		use(hdrln);
	}

	private void readPackFooter() throws IOException {
		sync();
		final byte[] actHash = packDigest.digest();

		final int c = fill(Source.INPUT, 20);
		final byte[] srcHash = new byte[20];
		System.arraycopy(buf, c, srcHash, 0, 20);
		use(20);

		if (!Arrays.equals(actHash, srcHash))
			throw new CorruptObjectException(
					JGitText.get().corruptObjectPackfileChecksumIncorrect);

		onPackFooter(srcHash);
	}

	// Cleanup all resources associated with our input parsing.
	private void endInput() {
		in = null;
	}

	// Read one entire object or delta from the input.
	private void indexOneObject() throws IOException {
		final long streamPosition = streamPosition();

		int hdrPtr = 0;
		int c = readFrom(Source.INPUT);
		hdrBuf[hdrPtr++] = (byte) c;

		final int typeCode = (c >> 4) & 7;
		long sz = c & 15;
		int shift = 4;
		while ((c & 0x80) != 0) {
			c = readFrom(Source.INPUT);
			hdrBuf[hdrPtr++] = (byte) c;
			sz += (c & 0x7f) << shift;
			shift += 7;
		}

		switch (typeCode) {
		case Constants.OBJ_COMMIT:
		case Constants.OBJ_TREE:
		case Constants.OBJ_BLOB:
		case Constants.OBJ_TAG:
			onBeginWholeObject(streamPosition, typeCode, sz);
			onObjectHeader(Source.INPUT, hdrBuf, 0, hdrPtr);
			whole(streamPosition, typeCode, sz);
			break;

		case Constants.OBJ_OFS_DELTA: {
			c = readFrom(Source.INPUT);
			hdrBuf[hdrPtr++] = (byte) c;
			long ofs = c & 127;
			while ((c & 128) != 0) {
				ofs += 1;
				c = readFrom(Source.INPUT);
				hdrBuf[hdrPtr++] = (byte) c;
				ofs <<= 7;
				ofs += (c & 127);
			}
			final long base = streamPosition - ofs;
			onBeginOfsDelta(streamPosition, base, sz);
			onObjectHeader(Source.INPUT, hdrBuf, 0, hdrPtr);
			inflateAndSkip(Source.INPUT, sz);
			UnresolvedDelta n = onEndDelta();
			n.position = streamPosition;
			n.next = baseByPos.put(base, n);
			deltaCount++;
			break;
		}

		case Constants.OBJ_REF_DELTA: {
			c = fill(Source.INPUT, 20);
			final ObjectId base = ObjectId.fromRaw(buf, c);
			System.arraycopy(buf, c, hdrBuf, hdrPtr, 20);
			hdrPtr += 20;
			use(20);
			DeltaChain r = baseById.get(base);
			if (r == null) {
				r = new DeltaChain(base);
				baseById.add(r);
			}
			onBeginRefDelta(streamPosition, base, sz);
			onObjectHeader(Source.INPUT, hdrBuf, 0, hdrPtr);
			inflateAndSkip(Source.INPUT, sz);
			UnresolvedDelta n = onEndDelta();
			n.position = streamPosition;
			r.add(n);
			deltaCount++;
			break;
		}

		default:
			throw new IOException(MessageFormat.format(
					JGitText.get().unknownObjectType, typeCode));
		}
	}

	private void whole(final long pos, final int type, final long sz)
			throws IOException {
		objectDigest.update(Constants.encodedTypeString(type));
		objectDigest.update((byte) ' ');
		objectDigest.update(Constants.encodeASCII(sz));
		objectDigest.update((byte) 0);

		boolean checkContentLater = false;
		if (type == Constants.OBJ_BLOB) {
			byte[] readBuffer = buffer();
			InputStream inf = inflate(Source.INPUT, sz);
			long cnt = 0;
			while (cnt < sz) {
				int r = inf.read(readBuffer);
				if (r <= 0)
					break;
				objectDigest.update(readBuffer, 0, r);
				cnt += r;
			}
			inf.close();
			tempObjectId.fromRaw(objectDigest.digest(), 0);
			checkContentLater = isCheckObjectCollisions()
					&& readCurs.has(tempObjectId);

		} else {
			final byte[] data = inflateAndReturn(Source.INPUT, sz);
			objectDigest.update(data);
			tempObjectId.fromRaw(objectDigest.digest(), 0);
			verifySafeObject(tempObjectId, type, data);
		}

		PackedObjectInfo obj = newInfo(tempObjectId, null, null);
		obj.setOffset(pos);
		onEndWholeObject(obj);
		addObjectAndTrack(obj);
		if (checkContentLater)
			deferredCheckBlobs.add(obj);
	}

	private void verifySafeObject(final AnyObjectId id, final int type,
			final byte[] data) throws IOException {
		if (objCheck != null) {
			try {
				objCheck.check(type, data);
			} catch (CorruptObjectException e) {
				throw new IOException(MessageFormat.format(
						JGitText.get().invalidObject, Constants
								.typeString(type), id.name(), e.getMessage()));
			}
		}

		if (isCheckObjectCollisions()) {
			try {
				final ObjectLoader ldr = readCurs.open(id, type);
				final byte[] existingData = ldr.getCachedBytes(data.length);
				if (!Arrays.equals(data, existingData)) {
					throw new IOException(MessageFormat.format(
							JGitText.get().collisionOn, id.name()));
				}
			} catch (MissingObjectException notLocal) {
				// This is OK, we don't have a copy of the object locally
				// but the API throws when we try to read it as usually its
				// an error to read something that doesn't exist.
			}
		}
	}

	private void doDeferredCheckBlobs() throws IOException {
		final byte[] readBuffer = buffer();
		final byte[] curBuffer = new byte[readBuffer.length];
		ObjectTypeAndSize info = new ObjectTypeAndSize();

		for (PackedObjectInfo obj : deferredCheckBlobs) {
			info = openDatabase(obj, info);

			if (info.type != Constants.OBJ_BLOB)
				throw new IOException(MessageFormat.format(
						JGitText.get().unknownObjectType, info.type));

			ObjectStream cur = readCurs.open(obj, info.type).openStream();
			try {
				long sz = info.size;
				if (cur.getSize() != sz)
					throw new IOException(MessageFormat.format(
							JGitText.get().collisionOn, obj.name()));
				InputStream pck = inflate(Source.DATABASE, sz);
				while (0 < sz) {
					int n = (int) Math.min(readBuffer.length, sz);
					IO.readFully(cur, curBuffer, 0, n);
					IO.readFully(pck, readBuffer, 0, n);
					for (int i = 0; i < n; i++) {
						if (curBuffer[i] != readBuffer[i])
							throw new IOException(MessageFormat.format(JGitText
									.get().collisionOn, obj.name()));
					}
					sz -= n;
				}
				pck.close();
			} finally {
				cur.close();
			}
		}
	}

	/** @return current position of the input stream being parsed. */
	private long streamPosition() {
		return bBase + bOffset;
	}

	private ObjectTypeAndSize openDatabase(PackedObjectInfo obj,
			ObjectTypeAndSize info) throws IOException {
		bOffset = 0;
		bAvail = 0;
		return seekDatabase(obj, info);
	}

	private ObjectTypeAndSize openDatabase(UnresolvedDelta delta,
			ObjectTypeAndSize info) throws IOException {
		bOffset = 0;
		bAvail = 0;
		return seekDatabase(delta, info);
	}

	// Consume exactly one byte from the buffer and return it.
	private int readFrom(final Source src) throws IOException {
		if (bAvail == 0)
			fill(src, 1);
		bAvail--;
		return buf[bOffset++] & 0xff;
	}

	// Consume cnt bytes from the buffer.
	private void use(final int cnt) {
		bOffset += cnt;
		bAvail -= cnt;
	}

	// Ensure at least need bytes are available in in {@link #buf}.
	private int fill(final Source src, final int need) throws IOException {
		while (bAvail < need) {
			int next = bOffset + bAvail;
			int free = buf.length - next;
			if (free + bAvail < need) {
				switch (src) {
				case INPUT:
					sync();
					break;
				case DATABASE:
					if (bAvail > 0)
						System.arraycopy(buf, bOffset, buf, 0, bAvail);
					bOffset = 0;
					break;
				}
				next = bAvail;
				free = buf.length - next;
			}
			switch (src) {
			case INPUT:
				next = in.read(buf, next, free);
				break;
			case DATABASE:
				next = readDatabase(buf, next, free);
				break;
			}
			if (next <= 0)
				throw new EOFException(JGitText.get().packfileIsTruncated);
			bAvail += next;
		}
		return bOffset;
	}

	// Store consumed bytes in {@link #buf} up to {@link #bOffset}.
	private void sync() throws IOException {
		packDigest.update(buf, 0, bOffset);
		onStoreStream(buf, 0, bOffset);
		if (bAvail > 0)
			System.arraycopy(buf, bOffset, buf, 0, bAvail);
		bBase += bOffset;
		bOffset = 0;
	}

	/** @return a temporary byte array for use by the caller. */
	protected byte[] buffer() {
		return tempBuffer;
	}

	/**
	 * Construct a PackedObjectInfo instance for this parser.
	 *
	 * @param id
	 *            identity of the object to be tracked.
	 * @param delta
	 *            if the object was previously an unresolved delta, this is the
	 *            delta object that was tracking it. Otherwise null.
	 * @param deltaBase
	 *            if the object was previously an unresolved delta, this is the
	 *            ObjectId of the base of the delta. The base may be outside of
	 *            the pack stream if the stream was a thin-pack.
	 * @return info object containing this object's data.
	 */
	protected PackedObjectInfo newInfo(AnyObjectId id, UnresolvedDelta delta,
			ObjectId deltaBase) {
		PackedObjectInfo oe = new PackedObjectInfo(id);
		if (delta != null)
			oe.setCRC(delta.crc);
		return oe;
	}

	/**
	 * Store bytes received from the raw stream.
	 * <p>
	 * This method is invoked during {@link #parse(ProgressMonitor)} as data is
	 * consumed from the incoming stream. Implementors may use this event to
	 * archive the raw incoming stream to the destination repository in large
	 * chunks, without paying attention to object boundaries.
	 * <p>
	 * The only component of the pack not supplied to this method is the last 20
	 * bytes of the pack that comprise the trailing SHA-1 checksum. Those are
	 * passed to {@link #onPackFooter(byte[])}.
	 *
	 * @param raw
	 *            buffer to copy data out of.
	 * @param pos
	 *            first offset within the buffer that is valid.
	 * @param len
	 *            number of bytes in the buffer that are valid.
	 * @throws IOException
	 *             the stream cannot be archived.
	 */
	protected abstract void onStoreStream(byte[] raw, int pos, int len)
			throws IOException;

	/**
	 * Store (and/or checksum) an object header.
	 * <p>
	 * Invoked after any of the {@code onBegin()} events. The entire header is
	 * supplied in a single invocation, before any object data is supplied.
	 *
	 * @param src
	 *            where the data came from
	 * @param raw
	 *            buffer to read data from.
	 * @param pos
	 *            first offset within buffer that is valid.
	 * @param len
	 *            number of bytes in buffer that are valid.
	 * @throws IOException
	 *             the stream cannot be archived.
	 */
	protected abstract void onObjectHeader(Source src, byte[] raw, int pos,
			int len) throws IOException;

	/**
	 * Store (and/or checksum) a portion of an object's data.
	 * <p>
	 * This method may be invoked multiple times per object, depending on the
	 * size of the object, the size of the parser's internal read buffer, and
	 * the alignment of the object relative to the read buffer.
	 * <p>
	 * Invoked after {@link #onObjectHeader(Source, byte[], int, int)}.
	 *
	 * @param src
	 *            where the data came from
	 * @param raw
	 *            buffer to read data from.
	 * @param pos
	 *            first offset within buffer that is valid.
	 * @param len
	 *            number of bytes in buffer that are valid.
	 * @throws IOException
	 *             the stream cannot be archived.
	 */
	protected abstract void onObjectData(Source src, byte[] raw, int pos,
			int len) throws IOException;

	/**
	 * Provide the implementation with the original stream's pack footer.
	 *
	 * @param hash
	 *            the trailing 20 bytes of the pack, this is a SHA-1 checksum of
	 *            all of the pack data.
	 * @throws IOException
	 *             the stream cannot be archived.
	 */
	protected abstract void onPackFooter(byte[] hash) throws IOException;

	/**
	 * Provide the implementation with a base that was outside of the pack.
	 * <p>
	 * This event only occurs on a thin pack for base objects that were outside
	 * of the pack and came from the local repository. Usually an implementation
	 * uses this event to compress the base and append it onto the end of the
	 * pack, so the pack stays self-contained.
	 *
	 * @param typeCode
	 *            type of the base object.
	 * @param data
	 *            complete content of the base object.
	 * @param info
	 *            packed object information for this base. Implementors must
	 *            populate the CRC and offset members if returning true.
	 * @return true if the {@code info} should be included in the object list
	 *         returned by {@link #getSortedObjectList(Comparator)}, false if it
	 *         should not be included.
	 * @throws IOException
	 *             the base could not be included into the pack.
	 */
	protected abstract boolean onAppendBase(int typeCode, byte[] data,
			PackedObjectInfo info) throws IOException;

	/**
	 * Event indicating a thin pack has been completely processed.
	 * <p>
	 * This event is invoked only if a thin pack has delta references to objects
	 * external from the pack. The event is called after all of those deltas
	 * have been resolved.
	 *
	 * @throws IOException
	 *             the pack cannot be archived.
	 */
	protected abstract void onEndThinPack() throws IOException;

	/**
	 * Reposition the database to re-read a previously stored object.
	 * <p>
	 * If the database is computing CRC-32 checksums for object data, it should
	 * reset its internal CRC instance during this method call.
	 *
	 * @param obj
	 *            the object position to begin reading from. This is from
	 *            {@link #newInfo(AnyObjectId, UnresolvedDelta, ObjectId)}.
	 * @param info
	 *            object to populate with type and size.
	 * @return the {@code info} object.
	 * @throws IOException
	 *             the database cannot reposition to this location.
	 */
	protected abstract ObjectTypeAndSize seekDatabase(PackedObjectInfo obj,
			ObjectTypeAndSize info) throws IOException;

	/**
	 * Reposition the database to re-read a previously stored object.
	 * <p>
	 * If the database is computing CRC-32 checksums for object data, it should
	 * reset its internal CRC instance during this method call.
	 *
	 * @param delta
	 *            the object position to begin reading from. This is an instance
	 *            previously returned by {@link #onEndDelta()}.
	 * @param info
	 *            object to populate with type and size.
	 * @return the {@code info} object.
	 * @throws IOException
	 *             the database cannot reposition to this location.
	 */
	protected abstract ObjectTypeAndSize seekDatabase(UnresolvedDelta delta,
			ObjectTypeAndSize info) throws IOException;

	/**
	 * Read from the database's current position into the buffer.
	 *
	 * @param dst
	 *            the buffer to copy read data into.
	 * @param pos
	 *            position within {@code dst} to start copying data into.
	 * @param cnt
	 *            ideal target number of bytes to read. Actual read length may
	 *            be shorter.
	 * @return number of bytes stored.
	 * @throws IOException
	 *             the database cannot be accessed.
	 */
	protected abstract int readDatabase(byte[] dst, int pos, int cnt)
			throws IOException;

	/**
	 * Check the current CRC matches the expected value.
	 * <p>
	 * This method is invoked when an object is read back in from the database
	 * and its data is used during delta resolution. The CRC is validated after
	 * the object has been fully read, allowing the parser to verify there was
	 * no silent data corruption.
	 * <p>
	 * Implementations are free to ignore this check by always returning true if
	 * they are performing other data integrity validations at a lower level.
	 *
	 * @param oldCRC
	 *            the prior CRC that was recorded during the first scan of the
	 *            object from the pack stream.
	 * @return true if the CRC matches; false if it does not.
	 */
	protected abstract boolean checkCRC(int oldCRC);

	/**
	 * Event notifying the start of an object stored whole (not as a delta).
	 *
	 * @param streamPosition
	 *            position of this object in the incoming stream.
	 * @param type
	 *            type of the object; one of {@link Constants#OBJ_COMMIT},
	 *            {@link Constants#OBJ_TREE}, {@link Constants#OBJ_BLOB}, or
	 *            {@link Constants#OBJ_TAG}.
	 * @param inflatedSize
	 *            size of the object when fully inflated. The size stored within
	 *            the pack may be larger or smaller, and is not yet known.
	 * @throws IOException
	 *             the object cannot be recorded.
	 */
	protected abstract void onBeginWholeObject(long streamPosition, int type,
			long inflatedSize) throws IOException;

	/**
	 * Event notifying the the current object.
	 *
	 *@param info
	 *            object information.
	 * @throws IOException
	 *             the object cannot be recorded.
	 */
	protected abstract void onEndWholeObject(PackedObjectInfo info)
			throws IOException;

	/**
	 * Event notifying start of a delta referencing its base by offset.
	 *
	 * @param deltaStreamPosition
	 *            position of this object in the incoming stream.
	 * @param baseStreamPosition
	 *            position of the base object in the incoming stream. The base
	 *            must be before the delta, therefore {@code baseStreamPosition
	 *            &lt; deltaStreamPosition}. This is <b>not</b> the position
	 *            returned by a prior end object event.
	 * @param inflatedSize
	 *            size of the delta when fully inflated. The size stored within
	 *            the pack may be larger or smaller, and is not yet known.
	 * @throws IOException
	 *             the object cannot be recorded.
	 */
	protected abstract void onBeginOfsDelta(long deltaStreamPosition,
			long baseStreamPosition, long inflatedSize) throws IOException;

	/**
	 * Event notifying start of a delta referencing its base by ObjectId.
	 *
	 * @param deltaStreamPosition
	 *            position of this object in the incoming stream.
	 * @param baseId
	 *            name of the base object. This object may be later in the
	 *            stream, or might not appear at all in the stream (in the case
	 *            of a thin-pack).
	 * @param inflatedSize
	 *            size of the delta when fully inflated. The size stored within
	 *            the pack may be larger or smaller, and is not yet known.
	 * @throws IOException
	 *             the object cannot be recorded.
	 */
	protected abstract void onBeginRefDelta(long deltaStreamPosition,
			AnyObjectId baseId, long inflatedSize) throws IOException;

	/**
	 * Event notifying the the current object.
	 *
	 *@return object information that must be populated with at least the
	 *         offset.
	 * @throws IOException
	 *             the object cannot be recorded.
	 */
	protected UnresolvedDelta onEndDelta() throws IOException {
		return new UnresolvedDelta();
	}

	/** Type and size information about an object in the database buffer. */
	public static class ObjectTypeAndSize {
		/** The type of the object. */
		public int type;

		/** The inflated size of the object. */
		public long size;
	}

	private void inflateAndSkip(final Source src, final long inflatedSize)
			throws IOException {
		final InputStream inf = inflate(src, inflatedSize);
		IO.skipFully(inf, inflatedSize);
		inf.close();
	}

	private byte[] inflateAndReturn(final Source src, final long inflatedSize)
			throws IOException {
		final byte[] dst = new byte[(int) inflatedSize];
		final InputStream inf = inflate(src, inflatedSize);
		IO.readFully(inf, dst, 0, dst.length);
		inf.close();
		return dst;
	}

	private InputStream inflate(final Source src, final long inflatedSize)
			throws IOException {
		inflater.open(src, inflatedSize);
		return inflater;
	}

	private static class DeltaChain extends ObjectId {
		UnresolvedDelta head;

		DeltaChain(final AnyObjectId id) {
			super(id);
		}

		UnresolvedDelta remove() {
			final UnresolvedDelta r = head;
			if (r != null)
				head = null;
			return r;
		}

		void add(final UnresolvedDelta d) {
			d.next = head;
			head = d;
		}
	}

	/** Information about an unresolved delta in this pack stream. */
	public static class UnresolvedDelta {
		long position;

		int crc;

		UnresolvedDelta next;

		/** @return offset within the input stream. */
		public long getOffset() {
			return position;
		}

		/** @return the CRC-32 checksum of the stored delta data. */
		public int getCRC() {
			return crc;
		}

		/**
		 * @param crc32
		 *            the CRC-32 checksum of the stored delta data.
		 */
		public void setCRC(int crc32) {
			crc = crc32;
		}
	}

	private static class DeltaVisit {
		final UnresolvedDelta delta;

		ObjectId id;

		byte[] data;

		DeltaVisit parent;

		UnresolvedDelta nextChild;

		DeltaVisit() {
			this.delta = null; // At the root of the stack we have a base.
		}

		DeltaVisit(DeltaVisit parent) {
			this.parent = parent;
			this.delta = parent.nextChild;
			parent.nextChild = delta.next;
		}

		DeltaVisit next() {
			// If our parent has no more children, discard it.
			if (parent != null && parent.nextChild == null) {
				parent.data = null;
				parent = parent.parent;
			}

			if (nextChild != null)
				return new DeltaVisit(this);

			// If we have no child ourselves, our parent must (if it exists),
			// due to the discard rule above. With no parent, we are done.
			if (parent != null)
				return new DeltaVisit(parent);
			return null;
		}
	}

	private void addObjectAndTrack(PackedObjectInfo oe) {
		entries[entryCount++] = oe;
		if (needNewObjectIds())
			newObjectIds.add(oe);
	}

	private class InflaterStream extends InputStream {
		private final Inflater inf;

		private final byte[] skipBuffer;

		private Source src;

		private long expectedSize;

		private long actualSize;

		private int p;

		InflaterStream() {
			inf = InflaterCache.get();
			skipBuffer = new byte[512];
		}

		void release() {
			inf.reset();
			InflaterCache.release(inf);
		}

		void open(Source source, long inflatedSize) throws IOException {
			src = source;
			expectedSize = inflatedSize;
			actualSize = 0;

			p = fill(src, 1);
			inf.setInput(buf, p, bAvail);
		}

		@Override
		public long skip(long toSkip) throws IOException {
			long n = 0;
			while (n < toSkip) {
				final int cnt = (int) Math.min(skipBuffer.length, toSkip - n);
				final int r = read(skipBuffer, 0, cnt);
				if (r <= 0)
					break;
				n += r;
			}
			return n;
		}

		@Override
		public int read() throws IOException {
			int n = read(skipBuffer, 0, 1);
			return n == 1 ? skipBuffer[0] & 0xff : -1;
		}

		@Override
		public int read(byte[] dst, int pos, int cnt) throws IOException {
			try {
				int n = 0;
				while (n < cnt) {
					int r = inf.inflate(dst, pos + n, cnt - n);
					if (r == 0) {
						if (inf.finished())
							break;
						if (inf.needsInput()) {
							onObjectData(src, buf, p, bAvail);
							use(bAvail);

							p = fill(src, 1);
							inf.setInput(buf, p, bAvail);
						} else {
							throw new CorruptObjectException(
									MessageFormat
											.format(
													JGitText.get().packfileCorruptionDetected,
													JGitText.get().unknownZlibError));
						}
					} else {
						n += r;
					}
				}
				actualSize += n;
				return 0 < n ? n : -1;
			} catch (DataFormatException dfe) {
				throw new CorruptObjectException(MessageFormat.format(JGitText
						.get().packfileCorruptionDetected, dfe.getMessage()));
			}
		}

		@Override
		public void close() throws IOException {
			// We need to read here to enter the loop above and pump the
			// trailing checksum into the Inflater. It should return -1 as the
			// caller was supposed to consume all content.
			//
			if (read(skipBuffer) != -1 || actualSize != expectedSize) {
				throw new CorruptObjectException(MessageFormat.format(JGitText
						.get().packfileCorruptionDetected,
						JGitText.get().wrongDecompressedLength));
			}

			int used = bAvail - inf.getRemaining();
			if (0 < used) {
				onObjectData(src, buf, p, used);
				use(used);
			}

			inf.reset();
		}
	}
}

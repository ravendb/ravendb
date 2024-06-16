/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Analyzer = Lucene.Net.Analysis.Analyzer;
using Document = Lucene.Net.Documents.Document;
using IndexingChain = Lucene.Net.Index.DocumentsWriter.IndexingChain;
using AlreadyClosedException = Lucene.Net.Store.AlreadyClosedException;
using BufferedIndexInput = Lucene.Net.Store.BufferedIndexInput;
using Directory = Lucene.Net.Store.Directory;
using Lock = Lucene.Net.Store.Lock;
using LockObtainFailedException = Lucene.Net.Store.LockObtainFailedException;
using Constants = Lucene.Net.Util.Constants;
using Query = Lucene.Net.Search.Query;
using Similarity = Lucene.Net.Search.Similarity;

namespace Lucene.Net.Index
{
	
	/// <summary>An <c>IndexWriter</c> creates and maintains an index.
	/// <p/>The <c>create</c> argument to the 
    /// <see cref="IndexWriter(Directory, Analyzer, bool, MaxFieldLength)">constructor</see> determines 
	/// whether a new index is created, or whether an existing index is
	/// opened.  Note that you can open an index with <c>create=true</c>
	/// even while readers are using the index.  The old readers will 
	/// continue to search the "point in time" snapshot they had opened, 
	/// and won't see the newly created index until they re-open.  There are
	/// also <see cref="IndexWriter(Directory, Analyzer, MaxFieldLength)">constructors</see>
	/// with no <c>create</c> argument which will create a new index
	/// if there is not already an index at the provided path and otherwise 
	/// open the existing index.<p/>
	/// <p/>In either case, documents are added with <see cref="AddDocument(Document)" />
	/// and removed with <see cref="DeleteDocuments(Term)" /> or
	/// <see cref="DeleteDocuments(Query)" />. A document can be updated with
	/// <see cref="UpdateDocument(Term, Document)" /> (which just deletes
	/// and then adds the entire document). When finished adding, deleting 
	/// and updating documents, <see cref="Close()" /> should be called.<p/>
	/// <a name="flush"></a>
	/// <p/>These changes are buffered in memory and periodically
	/// flushed to the <see cref="Directory" /> (during the above method
	/// calls).  A flush is triggered when there are enough
	/// buffered deletes (see <see cref="SetMaxBufferedDeleteTerms" />)
	/// or enough added documents since the last flush, whichever
	/// is sooner.  For the added documents, flushing is triggered
	/// either by RAM usage of the documents (see 
	/// <see cref="SetRAMBufferSizeMB" />) or the number of added documents.
	/// The default is to flush when RAM usage hits 16 MB.  For
	/// best indexing speed you should flush by RAM usage with a
	/// large RAM buffer.  Note that flushing just moves the
	/// internal buffered state in IndexWriter into the index, but
	/// these changes are not visible to IndexReader until either
	/// <see cref="Commit()" /> or <see cref="Close()" /> is called.  A flush may
	/// also trigger one or more segment merges which by default
	/// run with a background thread so as not to block the
	/// addDocument calls (see <a href="#mergePolicy">below</a>
	/// for changing the <see cref="MergeScheduler" />).
	/// <p/>
	/// If an index will not have more documents added for a while and optimal search
	/// performance is desired, then either the full <see cref="Optimize()" />
	/// method or partial <see cref="Optimize(int)" /> method should be
	/// called before the index is closed.
	/// <p/>
	/// Opening an <c>IndexWriter</c> creates a lock file for the directory in use. Trying to open
	/// another <c>IndexWriter</c> on the same directory will lead to a
	/// <see cref="LockObtainFailedException" />. The <see cref="LockObtainFailedException" />
	/// is also thrown if an IndexReader on the same directory is used to delete documents
	/// from the index.<p/>
	/// </summary>
	/// <summary><a name="deletionPolicy"></a>
	/// <p/>Expert: <c>IndexWriter</c> allows an optional
	/// <see cref="IndexDeletionPolicy" /> implementation to be
	/// specified.  You can use this to control when prior commits
	/// are deleted from the index.  The default policy is <see cref="KeepOnlyLastCommitDeletionPolicy" />
	/// which removes all prior
	/// commits as soon as a new commit is done (this matches
	/// behavior before 2.2).  Creating your own policy can allow
	/// you to explicitly keep previous "point in time" commits
	/// alive in the index for some time, to allow readers to
	/// refresh to the new commit without having the old commit
	/// deleted out from under them.  This is necessary on
	/// filesystems like NFS that do not support "delete on last
	/// close" semantics, which Lucene's "point in time" search
	/// normally relies on. <p/>
	/// <a name="mergePolicy"></a> <p/>Expert:
	/// <c>IndexWriter</c> allows you to separately change
	/// the <see cref="MergePolicy" /> and the <see cref="MergeScheduler" />.
	/// The <see cref="MergePolicy" /> is invoked whenever there are
	/// changes to the segments in the index.  Its role is to
	/// select which merges to do, if any, and return a <see cref="Net.Index.MergePolicy.MergeSpecification" />
	/// describing the merges.  It
	/// also selects merges to do for optimize().  (The default is
	/// <see cref="LogByteSizeMergePolicy" />.  Then, the <see cref="MergeScheduler" />
	/// is invoked with the requested merges and
	/// it decides when and how to run the merges.  The default is
	/// <see cref="ConcurrentMergeScheduler" />. <p/>
	/// <a name="OOME"></a><p/><b>NOTE</b>: if you hit an
	/// OutOfMemoryError then IndexWriter will quietly record this
	/// fact and block all future segment commits.  This is a
	/// defensive measure in case any internal state (buffered
	/// documents and deletions) were corrupted.  Any subsequent
	/// calls to <see cref="Commit()" /> will throw an
	/// IllegalStateException.  The only course of action is to
	/// call <see cref="Close()" />, which internally will call <see cref="Rollback()" />
	///, to undo any changes to the index since the
	/// last commit.  You can also just call <see cref="Rollback()" />
	/// directly.<p/>
	/// <a name="thread-safety"></a><p/><b>NOTE</b>: 
    /// <see cref="IndexWriter" /> instances are completely thread
	/// safe, meaning multiple threads can call any of its
	/// methods, concurrently.  If your application requires
	/// external synchronization, you should <b>not</b>
	/// synchronize on the <c>IndexWriter</c> instance as
	/// this may cause deadlock; use your own (non-Lucene) objects
	/// instead. <p/>
	/// <b>NOTE:</b> if you call
	/// <c>Thread.Interrupt()</c> on a thread that's within
	/// IndexWriter, IndexWriter will try to catch this (eg, if
	/// it's in a Wait() or Thread.Sleep()), and will then throw
	/// the unchecked exception <see cref="System.Threading.ThreadInterruptedException"/>
	/// and <b>clear</b> the interrupt status on the thread<p/>
	/// </summary>

    /*
    * Clarification: Check Points (and commits)
    * IndexWriter writes new index files to the directory without writing a new segments_N
    * file which references these new files. It also means that the state of 
    * the in memory SegmentInfos object is different than the most recent
    * segments_N file written to the directory.
    * 
    * Each time the SegmentInfos is changed, and matches the (possibly 
    * modified) directory files, we have a new "check point". 
    * If the modified/new SegmentInfos is written to disk - as a new 
    * (generation of) segments_N file - this check point is also an 
    * IndexCommit.
    * 
    * A new checkpoint always replaces the previous checkpoint and 
    * becomes the new "front" of the index. This allows the IndexFileDeleter 
    * to delete files that are referenced only by stale checkpoints.
    * (files that were created since the last commit, but are no longer
    * referenced by the "front" of the index). For this, IndexFileDeleter 
    * keeps track of the last non commit checkpoint.
    */
    public class IndexWriter : System.IDisposable
	{
		private void InitBlock()
		{
			similarity = Search.Similarity.Default;
			mergePolicy = new LogByteSizeMergePolicy(this);
			readerPool = new ReaderPool(this);
		}
		
		/// <summary> Default value for the write lock timeout (1,000).</summary>
		/// <seealso cref="DefaultWriteLockTimeout">
		/// </seealso>
		public static long WRITE_LOCK_TIMEOUT = 1000;
		
		private long writeLockTimeout = WRITE_LOCK_TIMEOUT;
		
		/// <summary> Name of the write lock in the index.</summary>
		public const System.String WRITE_LOCK_NAME = "write.lock";
		
		/// <summary> Value to denote a flush trigger is disabled</summary>
		public const int DISABLE_AUTO_FLUSH = - 1;
		
		/// <summary> Disabled by default (because IndexWriter flushes by RAM usage
		/// by default). Change using <see cref="SetMaxBufferedDocs(int)" />.
		/// </summary>
		public static readonly int DEFAULT_MAX_BUFFERED_DOCS = DISABLE_AUTO_FLUSH;
		
		/// <summary> Default value is 16 MB (which means flush when buffered
		/// docs consume 16 MB RAM).  Change using <see cref="SetRAMBufferSizeMB" />.
		/// </summary>
		public const double DEFAULT_RAM_BUFFER_SIZE_MB = 16.0;
		
		/// <summary> Disabled by default (because IndexWriter flushes by RAM usage
		/// by default). Change using <see cref="SetMaxBufferedDeleteTerms(int)" />.
		/// </summary>
		public static readonly int DEFAULT_MAX_BUFFERED_DELETE_TERMS = DISABLE_AUTO_FLUSH;
		
		/// <summary> Default value is 10,000. Change using <see cref="SetMaxFieldLength(int)" />.</summary>
		public const int DEFAULT_MAX_FIELD_LENGTH = 10000;
		
		/// <summary> Default value is 128. Change using <see cref="TermIndexInterval" />.</summary>
		public const int DEFAULT_TERM_INDEX_INTERVAL = 128;
		
		/// <summary> Absolute hard maximum length for a term.  If a term
		/// arrives from the analyzer longer than this length, it
		/// is skipped and a message is printed to infoStream, if
		/// set (see <see cref="SetInfoStream" />).
		/// </summary>
		public static readonly int MAX_TERM_LENGTH;
		
		// The normal read buffer size defaults to 1024, but
		// increasing this during merging seems to yield
		// performance gains.  However we don't want to increase
		// it too much because there are quite a few
		// BufferedIndexInputs created during merging.  See
		// LUCENE-888 for details.
		private const int MERGE_READ_BUFFER_SIZE = 4096;
		
		// Used for printing messages
		private static System.Object MESSAGE_ID_LOCK = new System.Object();
		private static int MESSAGE_ID = 0;
		private int messageID = - 1;
		private volatile bool hitOOM;
		
		private Directory directory; // where this index resides
		private Analyzer analyzer; // how to analyze text
		
		private Similarity similarity; // how to normalize
		
		private volatile uint changeCount; // increments every time a change is completed
		private long lastCommitChangeCount; // last changeCount that was committed
		
		private SegmentInfos rollbackSegmentInfos; // segmentInfos we will fallback to if the commit fails
		private HashMap<SegmentInfo, int?> rollbackSegments;
		
		internal volatile SegmentInfos pendingCommit; // set when a commit is pending (after prepareCommit() & before commit())
		internal volatile uint pendingCommitChangeCount;
		
		private SegmentInfos localRollbackSegmentInfos; // segmentInfos we will fallback to if the commit fails
		private int localFlushedDocCount; // saved docWriter.getFlushedDocCount during local transaction
		
		private SegmentInfos segmentInfos = new SegmentInfos(); // the segments
        private int optimizeMaxNumSegments;

		internal DocumentsWriter docWriter;
		private IndexFileDeleter deleter;

        private ISet<SegmentInfo> segmentsToOptimize = Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet<SegmentInfo>(); // used by optimize to note those needing optimization
		
		private Lock writeLock;
		
		private int termIndexInterval = DEFAULT_TERM_INDEX_INTERVAL;
		
		private bool closed;
		private bool closing;
		
		// Holds all SegmentInfo instances currently involved in
		// merges
        private HashSet<SegmentInfo> mergingSegments = new HashSet<SegmentInfo>();
		
		private MergePolicy mergePolicy;
		private MergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
        private LinkedList<MergePolicy.OneMerge> pendingMerges = new LinkedList<MergePolicy.OneMerge>();
		private ISet<MergePolicy.OneMerge> runningMerges = Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet<MergePolicy.OneMerge>();
		private IList<MergePolicy.OneMerge> mergeExceptions = new List<MergePolicy.OneMerge>();
		private long mergeGen;
		private bool stopMerges;

        public int PendingMergesCount => pendingMerges.Count;

		internal int flushCount;
		private int flushDeletesCount;
		
		// Used to only allow one addIndexes to proceed at once
		// TODO: use ReadWriteLock once we are on 5.0
		private int readCount; // count of how many threads are holding read lock
		private ThreadClass writeThread; // non-null if any thread holds write lock
		internal ReaderPool readerPool;
		private int upgradeCount;

		private OptimizeScope _optimizeScope;

		private int readerTermsIndexDivisor = IndexReader.DEFAULT_TERMS_INDEX_DIVISOR;
		
		// This is a "write once" variable (like the organic dye
		// on a DVD-R that may or may not be heated by a laser and
		// then cooled to permanently record the event): it's
		// false, until getReader() is called for the first time,
		// at which point it's switched to true and never changes
		// back to false.  Once this is true, we hold open and
		// reuse SegmentReader instances internally for applying
		// deletes, doing merges, and reopening near real-time
		// readers.
		private volatile bool poolReaders;
		
		/// <summary> Expert: returns a readonly reader, covering all committed as well as
		/// un-committed changes to the index. This provides "near real-time"
		/// searching, in that changes made during an IndexWriter session can be
		/// quickly made available for searching without closing the writer nor
		/// calling <see cref="Commit()" />.
		/// 
		/// <p/>
		/// Note that this is functionally equivalent to calling {#commit} and then
		/// using <see cref="IndexReader.Open(Lucene.Net.Store.Directory, bool)" /> to open a new reader. But the turarnound
		/// time of this method should be faster since it avoids the potentially
		/// costly <see cref="Commit()" />.
		/// <p/>
		/// 
        /// You must close the <see cref="IndexReader" /> returned by  this method once you are done using it.
        /// 
		/// <p/>
		/// It's <i>near</i> real-time because there is no hard
		/// guarantee on how quickly you can get a new reader after
		/// making changes with IndexWriter.  You'll have to
		/// experiment in your situation to determine if it's
		/// faster enough.  As this is a new and experimental
		/// feature, please report back on your findings so we can
		/// learn, improve and iterate.<p/>
		/// 
		/// <p/>The resulting reader suppports <see cref="IndexReader.Reopen()" />
		///, but that call will simply forward
		/// back to this method (though this may change in the
		/// future).<p/>
		/// 
		/// <p/>The very first time this method is called, this
		/// writer instance will make every effort to pool the
		/// readers that it opens for doing merges, applying
		/// deletes, etc.  This means additional resources (RAM,
		/// file descriptors, CPU time) will be consumed.<p/>
		/// 
		/// <p/>For lower latency on reopening a reader, you should call <see cref="MergedSegmentWarmer" /> 
        /// to call <see cref="MergedSegmentWarmer" /> to
		/// pre-warm a newly merged segment before it's committed
		/// to the index. This is important for minimizing index-to-search 
        /// delay after a large merge.
		/// 
		/// <p/>If an addIndexes* call is running in another thread,
		/// then this reader will only search those segments from
		/// the foreign index that have been successfully copied
		/// over, so far<p/>.
		/// 
		/// <p/><b>NOTE</b>: Once the writer is closed, any
		/// outstanding readers may continue to be used.  However,
		/// if you attempt to reopen any of those readers, you'll
		/// hit an <see cref="AlreadyClosedException" />.<p/>
		/// 
		/// <p/><b>NOTE:</b> This API is experimental and might
		/// change in incompatible ways in the next release.<p/>
		/// 
		/// </summary>
		/// <returns> IndexReader that covers entire index plus all
		/// changes made so far by this IndexWriter instance
		/// 
		/// </returns>
		/// <throws>  IOException </throws>
		public virtual IndexReader GetReader(IState state)
		{
            return GetReader(readerTermsIndexDivisor, state);
		}
		
		/// <summary>Expert: like <see cref="GetReader()" />, except you can
		/// specify which termInfosIndexDivisor should be used for
		/// any newly opened readers.
		/// </summary>
		/// <param name="termInfosIndexDivisor">Subsambles which indexed
		/// terms are loaded into RAM. This has the same effect as <see cref="IndexWriter.TermIndexInterval" />
		/// except that setting
		/// must be done at indexing time while this setting can be
		/// set per reader.  When set to N, then one in every
		/// N*termIndexInterval terms in the index is loaded into
		/// memory.  By setting this to a value > 1 you can reduce
		/// memory usage, at the expense of higher latency when
		/// loading a TermInfo.  The default value is 1.  Set this
		/// to -1 to skip loading the terms index entirely. 
		/// </param>
		public virtual IndexReader GetReader(int termInfosIndexDivisor, IState state)
		{
            EnsureOpen();

			if (infoStream != null)
			{
				Message("flush at getReader");
			}
			
			// Do this up front before flushing so that the readers
			// obtained during this flush are pooled, the first time
			// this method is called:
			poolReaders = true;
			
			// Prevent segmentInfos from changing while opening the
			// reader; in theory we could do similar retry logic,
			// just like we do when loading segments_N
            IndexReader r;
			lock (this)
			{
                Flush(false, true, true, state);
                r = new ReadOnlyDirectoryReader(this, segmentInfos, termInfosIndexDivisor, state);
			}
            MaybeMerge(state);
            return r;
		}
		
		/// <summary>Holds shared SegmentReader instances. IndexWriter uses
		/// SegmentReaders for 1) applying deletes, 2) doing
		/// merges, 3) handing out a real-time reader.  This pool
		/// reuses instances of the SegmentReaders in all these
		/// places if it is in "near real-time mode" (getReader()
		/// has been called on this instance). 
		/// </summary>
		
		internal class ReaderPool : IDisposable
		{
			public ReaderPool(IndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(IndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private IndexWriter enclosingInstance;
			public IndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}

            private HashMap<SegmentInfo, SegmentReader> readerMap = new HashMap<SegmentInfo, SegmentReader>();
			
			/// <summary>Forcefully clear changes for the specifed segments,
			/// and remove from the pool.   This is called on succesful merge. 
			/// </summary>
			internal virtual void  Clear(SegmentInfos infos)
			{
				lock (this)
				{
					if (infos == null)
					{
                        foreach(KeyValuePair<SegmentInfo, SegmentReader> ent in readerMap)
						{
							ent.Value.hasChanges = false;
						}
					}
					else
					{
                        foreach(SegmentInfo info in infos)
						{
							if (readerMap.ContainsKey(info))
							{
								readerMap[info].hasChanges = false;
							}
						}
					}
				}
			}
			
			// used only by asserts
			public virtual bool InfoIsLive(SegmentInfo info)
			{
				lock (this)
				{
					int idx = Enclosing_Instance.segmentInfos.IndexOf(info);
					System.Diagnostics.Debug.Assert(idx != -1);
                    System.Diagnostics.Debug.Assert(Enclosing_Instance.segmentInfos[idx] == info);
					return true;
				}
			}
			
			public virtual SegmentInfo MapToLive(SegmentInfo info)
			{
				lock (this)
				{
					int idx = Enclosing_Instance.segmentInfos.IndexOf(info);
					if (idx != - 1)
					{
						info = Enclosing_Instance.segmentInfos[idx];
					}
					return info;
				}
			}
			
			/// <summary> Release the segment reader (i.e. decRef it and close if there
			/// are no more references.
			/// </summary>
			/// <param name="sr">
			/// </param>
			/// <throws>  IOException </throws>
			public virtual void  Release(SegmentReader sr, IState state)
			{
				lock (this)
				{
					Release(sr, false, state);
				}
			}

		    /// <summary> Release the segment reader (i.e. decRef it and close if there
		    /// are no more references.
		    /// </summary>
		    /// <param name="sr">
		    /// </param>
		    /// <param name="drop"></param>
		    /// <throws>  IOException </throws>
		    public virtual void  Release(SegmentReader sr, bool drop, IState state)
			{
				lock (this)
				{
					
					bool pooled = readerMap.ContainsKey(sr.SegmentInfo);

                    System.Diagnostics.Debug.Assert(!pooled || readerMap[sr.SegmentInfo] == sr);

                    // Drop caller's ref; for an external reader (not
                    // pooled), this decRef will close it
					sr.DecRef(state);
					
					if (pooled && (drop || (!Enclosing_Instance.poolReaders && sr.RefCount == 1)))
					{
                        // We invoke deleter.checkpoint below, so we must be
                        // sync'd on IW if there are changes:
						
                        // TODO: Java 1.5 has this, .NET can't.
						// System.Diagnostics.Debug.Assert(!sr.hasChanges || Thread.holdsLock(enclosingInstance));

                        // Discard (don't save) changes when we are dropping
                        // the reader; this is used only on the sub-readers
                        // after a successful merge.
                        if (drop)
                        {
                            sr.CloseWithoutCommit();
                            readerMap.Remove(sr.SegmentInfo);
							return;
                        }

                        if (sr.hasChanges)
                        {
							sr.Commit(state);

                            // Must checkpoint w/ deleter, because this
                            // segment reader will have created new _X_N.del
                            // file.
                            enclosingInstance.deleter.Checkpoint(enclosingInstance.segmentInfos, false, state);
                        }
					}
				}
			}

            /// <summary>Remove all our references to readers, and commits
            /// any pending changes. 
            /// </summary>
		    public void Dispose()
		    {
		        Dispose(true);
		    }

            protected void Dispose(bool disposing)
            {
                if (disposing)
                {
                    // We invoke deleter.checkpoint below, so we must be
                    // sync'd on IW:
                    // TODO: assert Thread.holdsLock(IndexWriter.this);
                    // TODO: Should this class have bool _isDisposed?
                    lock (this)
                    {
                        var state = StateHolder.Current.Value;

                        //var toRemove = new List<SegmentInfo>();
                        foreach (var ent in readerMap)
                        {
                            SegmentReader sr = ent.Value;
                            if (sr.hasChanges)
                            {
                                System.Diagnostics.Debug.Assert(InfoIsLive(sr.SegmentInfo));
                                sr.DoCommit(null, state);
                                // Must checkpoint w/ deleter, because this
                                // segment reader will have created new _X_N.del
                                // file.
                                enclosingInstance.deleter.Checkpoint(enclosingInstance.segmentInfos, false, state);
                            }

                            //toRemove.Add(ent.Key);

                            // NOTE: it is allowed that this decRef does not
                            // actually close the SR; this can happen when a
                            // near real-time reader is kept open after the
                            // IndexWriter instance is closed
                            sr.DecRef(state);
                        }

                        //foreach (var key in toRemove)
                        //    readerMap.Remove(key);
                        readerMap.Clear();
                    }
                }
            }
			
			/// <summary> Commit all segment reader in the pool.</summary>
			/// <throws>  IOException </throws>
			internal virtual void  Commit(IState state)
			{
                // We invoke deleter.checkpoint below, so we must be
                // sync'd on IW:
                // TODO: assert Thread.holdsLock(IndexWriter.this);
                lock (this)
				{
                    foreach(KeyValuePair<SegmentInfo,SegmentReader> ent in readerMap)
					{
						SegmentReader sr = ent.Value;
						if (sr.hasChanges)
						{
							System.Diagnostics.Debug.Assert(InfoIsLive(sr.SegmentInfo));
							sr.DoCommit(null, state);
                            // Must checkpoint w/ deleter, because this
                            // segment reader will have created new _X_N.del
                            // file.
                            enclosingInstance.deleter.Checkpoint(enclosingInstance.segmentInfos, false, state);
						}
					}
				}
			}
			
			/// <summary> Returns a ref to a clone.  NOTE: this clone is not
			/// enrolled in the pool, so you should simply close()
			/// it when you're done (ie, do not call release()).
			/// </summary>
			public virtual SegmentReader GetReadOnlyClone(SegmentInfo info, bool doOpenStores, int termInfosIndexDivisor, IState state)
			{
				lock (this)
				{
					SegmentReader sr = Get(info, doOpenStores, BufferedIndexInput.BUFFER_SIZE, termInfosIndexDivisor, state);
					try
					{
						return (SegmentReader) sr.Clone(true, state);
					}
					finally
					{
						sr.DecRef(state);
					}
				}
			}
			
			/// <summary> Obtain a SegmentReader from the readerPool.  The reader
			/// must be returned by calling <see cref="Release(SegmentReader)" />
			/// </summary>
			/// <seealso cref="Release(SegmentReader)">
			/// </seealso>
			/// <param name="info">
			/// </param>
			/// <param name="doOpenStores">
			/// </param>
			/// <throws>  IOException </throws>
			public virtual SegmentReader Get(SegmentInfo info, bool doOpenStores, IState state)
			{
				lock (this)
				{
                    return Get(info, doOpenStores, BufferedIndexInput.BUFFER_SIZE, enclosingInstance.readerTermsIndexDivisor, state);
				}
			}
			/// <summary> Obtain a SegmentReader from the readerPool.  The reader
			/// must be returned by calling <see cref="Release(SegmentReader)" />
			/// 
			/// </summary>
			/// <seealso cref="Release(SegmentReader)">
			/// </seealso>
			/// <param name="info">
			/// </param>
			/// <param name="doOpenStores">
			/// </param>
			/// <param name="readBufferSize">
			/// </param>
			/// <param name="termsIndexDivisor">
			/// </param>
			/// <throws>  IOException </throws>
			public virtual SegmentReader Get(SegmentInfo info, bool doOpenStores, int readBufferSize, int termsIndexDivisor, IState state)
			{
				lock (this)
				{
					if (Enclosing_Instance.poolReaders)
					{
						readBufferSize = BufferedIndexInput.BUFFER_SIZE;
					}
					
					SegmentReader sr = readerMap[info];
					if (sr == null)
					{
						// TODO: we may want to avoid doing this while
						// synchronized
						// Returns a ref, which we xfer to readerMap:

						sr = SegmentReader.Get(false, info.dir, info, readBufferSize, doOpenStores, termsIndexDivisor, state);
                        if (info.dir == enclosingInstance.directory)
                        {
                            // Only pool if reader is not external
                            readerMap[info]=sr;
                        }
					}
					else
					{
						if (doOpenStores)
						{
							sr.OpenDocStores(state);
						}
						if (termsIndexDivisor != - 1 && !sr.TermsIndexLoaded())
						{
							// If this reader was originally opened because we
							// needed to merge it, we didn't load the terms
							// index.  But now, if the caller wants the terms
							// index (eg because it's doing deletes, or an NRT
							// reader is being opened) we ask the reader to
							// load its terms index.
							sr.LoadTermsIndex(termsIndexDivisor, state);
						}
					}
					
					// Return a ref to our caller
                    if (info.dir == enclosingInstance.directory)
                    {
                        // Only incRef if we pooled (reader is not external)
                        sr.IncRef();
                    }
					return sr;
				}
			}
			
			// Returns a ref
			public virtual SegmentReader GetIfExists(SegmentInfo info)
			{
				lock (this)
				{
					SegmentReader sr = readerMap[info];
					if (sr != null)
					{
						sr.IncRef();
					}
					return sr;
				}
			}
		}
		
		/// <summary> Obtain the number of deleted docs for a pooled reader.
		/// If the reader isn't being pooled, the segmentInfo's 
		/// delCount is returned.
		/// </summary>
		public virtual int NumDeletedDocs(SegmentInfo info, IState state)
		{
			SegmentReader reader = readerPool.GetIfExists(info);
			try
			{
				if (reader != null)
				{
					return reader.NumDeletedDocs;
				}
				else
				{
					return info.GetDelCount(state);
				}
			}
			finally
			{
				if (reader != null)
				{
					readerPool.Release(reader, state);
				}
			}
		}
		
		internal virtual void  AcquireWrite()
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(writeThread != ThreadClass.Current());
				while (writeThread != null || readCount > 0)
					DoWait();
				
				// We could have been closed while we were waiting:
				EnsureOpen();
				
				writeThread = ThreadClass.Current();
			}
		}
		
		internal virtual void  ReleaseWrite()
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(ThreadClass.Current() == writeThread);
				writeThread = null;
				System.Threading.Monitor.PulseAll(this);
			}
		}
		
		internal virtual void  AcquireRead()
		{
			lock (this)
			{
				ThreadClass current = ThreadClass.Current();
				while (writeThread != null && writeThread != current)
					DoWait();
				
				readCount++;
			}
		}
		
		// Allows one readLock to upgrade to a writeLock even if
		// there are other readLocks as long as all other
		// readLocks are also blocked in this method:
		internal virtual void  UpgradeReadToWrite()
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(readCount > 0);
				upgradeCount++;
				while (readCount > upgradeCount || writeThread != null)
				{
					DoWait();
				}
				
				writeThread = ThreadClass.Current();
				readCount--;
				upgradeCount--;
			}
		}
		
		internal virtual void  ReleaseRead()
		{
			lock (this)
			{
				readCount--;
				System.Diagnostics.Debug.Assert(readCount >= 0);
				System.Threading.Monitor.PulseAll(this);
			}
		}
		
		internal bool IsOpen(bool includePendingClose)
		{
			lock (this)
			{
				return !(closed || (includePendingClose && closing));
			}
		}
		
		/// <summary> Used internally to throw an <see cref="AlreadyClosedException" />
		/// if this IndexWriter has been
		/// closed.
		/// </summary>
		/// <throws>  AlreadyClosedException if this IndexWriter is </throws>
		protected internal void  EnsureOpen(bool includePendingClose)
		{
			lock (this)
			{
				if (!IsOpen(includePendingClose))
				{
					throw new AlreadyClosedException("this IndexWriter is closed");
				}
			}
		}
		
		protected internal void  EnsureOpen()
		{
			lock (this)
			{
				EnsureOpen(true);
			}
		}
		
		/// <summary> Prints a message to the infoStream (if non-null),
		/// prefixed with the identifying information for this
		/// writer and the thread that's calling it.
		/// </summary>
		public virtual void  Message(System.String message)
		{
			if (infoStream != null)
                infoStream.WriteLine("IW " + messageID + " [" + DateTime.Now.ToString() + "; " + ThreadClass.Current().Name + "]: " + message);
		}
		
		private void  SetMessageID(System.IO.StreamWriter infoStream)
		{
			lock (this)
			{
				if (infoStream != null && messageID == - 1)
				{
					lock (MESSAGE_ID_LOCK)
					{
						messageID = MESSAGE_ID++;
					}
				}
				this.infoStream = infoStream;
			}
		}

		/// <summary>
		/// Contains current OptimizeScope
		/// </summary>
		public OptimizeScope OptimizeScope
		{
			get
			{
				lock (this)
				{
					return _optimizeScope;
				}
			}
		}
		
	    /// <summary> Casts current mergePolicy to LogMergePolicy, and throws
	    /// an exception if the mergePolicy is not a LogMergePolicy.
	    /// </summary>
	    private LogMergePolicy LogMergePolicy
	    {
	        get
	        {
	            if (mergePolicy is LogMergePolicy)
	                return (LogMergePolicy) mergePolicy;

	            throw new System.ArgumentException(
	                "this method can only be called when the merge policy is the default LogMergePolicy");
	        }
	    }

	    /// <summary><p/>Gets or sets the current setting of whether newly flushed
	    /// segments will use the compound file format.  Note that
	    /// this just returns the value previously set with
	    /// setUseCompoundFile(boolean), or the default value
	    /// (true).  You cannot use this to query the status of
	    /// previously flushed segments.<p/>
	    /// 
	    /// <p/>Note that this method is a convenience method: it
	    /// just calls mergePolicy.getUseCompoundFile as long as
	    /// mergePolicy is an instance of <see cref="LogMergePolicy" />.
	    /// Otherwise an IllegalArgumentException is thrown.<p/>
	    /// 
	    /// </summary>
	    public virtual bool UseCompoundFile
	    {
	        get { return LogMergePolicy.GetUseCompoundFile(); }
	        set
	        {
	            LogMergePolicy.SetUseCompoundFile(value);
	            LogMergePolicy.SetUseCompoundDocStore(value);
	        }
	    }

	    /// <summary>Expert: Set the Similarity implementation used by this IndexWriter.
		/// </summary>
		public virtual void  SetSimilarity(Similarity similarity)
		{
			EnsureOpen();
			this.similarity = similarity;
			docWriter.SetSimilarity(similarity);
		}

	    /// <summary>Expert: Return the Similarity implementation used by this IndexWriter.
	    /// 
	    /// <p/>This defaults to the current value of <see cref="Search.Similarity.Default" />.
	    /// </summary>
	    public virtual Similarity Similarity
	    {
	        get
	        {
	            EnsureOpen();
	            return this.similarity;
	        }
	    }


        /// <summary>Expert: Gets or sets the interval between indexed terms.  Large values cause less
        /// memory to be used by IndexReader, but slow random-access to terms.  Small
        /// values cause more memory to be used by an IndexReader, and speed
        /// random-access to terms.
        /// 
        /// This parameter determines the amount of computation required per query
        /// term, regardless of the number of documents that contain that term.  In
        /// particular, it is the maximum number of other terms that must be
        /// scanned before a term is located and its frequency and position information
        /// may be processed.  In a large index with user-entered query terms, query
        /// processing time is likely to be dominated not by term lookup but rather
        /// by the processing of frequency and positional data.  In a small index
        /// or when many uncommon query terms are generated (e.g., by wildcard
        /// queries) term lookup may become a dominant cost.
        /// 
        /// In particular, <c>numUniqueTerms/interval</c> terms are read into
        /// memory by an IndexReader, and, on average, <c>interval/2</c> terms
        /// must be scanned for each random term access.
        /// 
        /// </summary>
        /// <seealso cref="DEFAULT_TERM_INDEX_INTERVAL">
        /// </seealso>
	    public virtual int TermIndexInterval
	    {
	        get
	        {
	            // We pass false because this method is called by SegmentMerger while we are in the process of closing
	            EnsureOpen(false);
	            return termIndexInterval;
	        }
	        set
	        {
	            EnsureOpen();
	            this.termIndexInterval = value;
	        }
	    }

	    /// <summary> Constructs an IndexWriter for the index in <c>d</c>.
		/// Text will be analyzed with <c>a</c>.  If <c>create</c>
		/// is true, then a new, empty index will be created in
		/// <c>d</c>, replacing the index already there, if any.
		/// 
		/// </summary>
		/// <param name="d">the index directory
		/// </param>
		/// <param name="a">the analyzer to use
		/// </param>
		/// <param name="create"><c>true</c> to create the index or overwrite
		/// the existing one; <c>false</c> to append to the existing
		/// index
		/// </param>
		/// <param name="mfl">Maximum field length in number of terms/tokens: LIMITED, UNLIMITED, or user-specified
		/// via the MaxFieldLength constructor.
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  LockObtainFailedException if another writer </throws>
		/// <summary>  has this index open (<c>write.lock</c> could not
		/// be obtained)
		/// </summary>
		/// <throws>  IOException if the directory cannot be read/written to, or </throws>
		/// <summary>  if it does not exist and <c>create</c> is
		/// <c>false</c> or if there is any other low-level
		/// IO error
		/// </summary>
		public IndexWriter(Directory d, Analyzer a, bool create, MaxFieldLength mfl, IState state)
		{
			InitBlock();
			Init(d, a, create, null, mfl.Limit, null, null, state);
        }
		
		/// <summary> Constructs an IndexWriter for the index in
		/// <c>d</c>, first creating it if it does not
		/// already exist.  
		/// 
		/// </summary>
		/// <param name="d">the index directory
		/// </param>
		/// <param name="a">the analyzer to use
		/// </param>
		/// <param name="mfl">Maximum field length in number of terms/tokens: LIMITED, UNLIMITED, or user-specified
		/// via the MaxFieldLength constructor.
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  LockObtainFailedException if another writer </throws>
		/// <summary>  has this index open (<c>write.lock</c> could not
		/// be obtained)
		/// </summary>
		/// <throws>  IOException if the directory cannot be </throws>
		/// <summary>  read/written to or if there is any other low-level
		/// IO error
		/// </summary>
		public IndexWriter(Directory d, Analyzer a, MaxFieldLength mfl, IState state)
		{
			InitBlock();
			Init(d, a, null, mfl.Limit, null, null, state);
		}
		
		/// <summary> Expert: constructs an IndexWriter with a custom <see cref="IndexDeletionPolicy" />
		///, for the index in <c>d</c>,
		/// first creating it if it does not already exist.  Text
		/// will be analyzed with <c>a</c>.
		/// 
		/// </summary>
		/// <param name="d">the index directory
		/// </param>
		/// <param name="a">the analyzer to use
		/// </param>
		/// <param name="deletionPolicy">see <a href="#deletionPolicy">above</a>
		/// </param>
		/// <param name="mfl">whether or not to limit field lengths
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  LockObtainFailedException if another writer </throws>
		/// <summary>  has this index open (<c>write.lock</c> could not
		/// be obtained)
		/// </summary>
		/// <throws>  IOException if the directory cannot be </throws>
		/// <summary>  read/written to or if there is any other low-level
		/// IO error
		/// </summary>
		public IndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IState state)
		{
			InitBlock();
			Init(d, a, deletionPolicy, mfl.Limit, null, null, state);
		}
		
		/// <summary> Expert: constructs an IndexWriter with a custom <see cref="IndexDeletionPolicy" />
		///, for the index in <c>d</c>.
		/// Text will be analyzed with <c>a</c>.  If
		/// <c>create</c> is true, then a new, empty index
		/// will be created in <c>d</c>, replacing the index
		/// already there, if any.
		/// 
		/// </summary>
		/// <param name="d">the index directory
		/// </param>
		/// <param name="a">the analyzer to use
		/// </param>
		/// <param name="create"><c>true</c> to create the index or overwrite
		/// the existing one; <c>false</c> to append to the existing
		/// index
		/// </param>
		/// <param name="deletionPolicy">see <a href="#deletionPolicy">above</a>
		/// </param>
		/// <param name="mfl"><see cref="Lucene.Net.Index.IndexWriter.MaxFieldLength" />, whether or not to limit field lengths.  Value is in number of terms/tokens
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  LockObtainFailedException if another writer </throws>
		/// <summary>  has this index open (<c>write.lock</c> could not
		/// be obtained)
		/// </summary>
		/// <throws>  IOException if the directory cannot be read/written to, or </throws>
		/// <summary>  if it does not exist and <c>create</c> is
		/// <c>false</c> or if there is any other low-level
		/// IO error
		/// </summary>
		public IndexWriter(Directory d, Analyzer a, bool create, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IState state)
		{
			InitBlock();
			Init(d, a, create, deletionPolicy, mfl.Limit, null, null, state);
		}
		
		/// <summary> Expert: constructs an IndexWriter with a custom <see cref="IndexDeletionPolicy" />
		/// and <see cref="IndexingChain" />, 
		/// for the index in <c>d</c>.
		/// Text will be analyzed with <c>a</c>.  If
		/// <c>create</c> is true, then a new, empty index
		/// will be created in <c>d</c>, replacing the index
		/// already there, if any.
		/// 
		/// </summary>
		/// <param name="d">the index directory
		/// </param>
		/// <param name="a">the analyzer to use
		/// </param>
		/// <param name="create"><c>true</c> to create the index or overwrite
		/// the existing one; <c>false</c> to append to the existing
		/// index
		/// </param>
		/// <param name="deletionPolicy">see <a href="#deletionPolicy">above</a>
		/// </param>
		/// <param name="mfl">whether or not to limit field lengths, value is in number of terms/tokens.  See <see cref="Lucene.Net.Index.IndexWriter.MaxFieldLength" />.
		/// </param>
		/// <param name="indexingChain">the <see cref="DocConsumer" /> chain to be used to 
		/// process documents
		/// </param>
		/// <param name="commit">which commit to open
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  LockObtainFailedException if another writer </throws>
		/// <summary>  has this index open (<c>write.lock</c> could not
		/// be obtained)
		/// </summary>
		/// <throws>  IOException if the directory cannot be read/written to, or </throws>
		/// <summary>  if it does not exist and <c>create</c> is
		/// <c>false</c> or if there is any other low-level
		/// IO error
		/// </summary>
		internal IndexWriter(Directory d, Analyzer a, bool create, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IndexingChain indexingChain, IndexCommit commit, IState state)
		{
			InitBlock();
			Init(d, a, create, deletionPolicy, mfl.Limit, indexingChain, commit, state);
		}
		
		/// <summary> Expert: constructs an IndexWriter on specific commit
		/// point, with a custom <see cref="IndexDeletionPolicy" />, for
		/// the index in <c>d</c>.  Text will be analyzed
		/// with <c>a</c>.
		/// 
		/// <p/> This is only meaningful if you've used a <see cref="IndexDeletionPolicy" />
		/// in that past that keeps more than
		/// just the last commit.
		/// 
		/// <p/>This operation is similar to <see cref="Rollback()" />,
		/// except that method can only rollback what's been done
		/// with the current instance of IndexWriter since its last
		/// commit, whereas this method can rollback to an
		/// arbitrary commit point from the past, assuming the
		/// <see cref="IndexDeletionPolicy" /> has preserved past
		/// commits.
		/// 
		/// </summary>
		/// <param name="d">the index directory
		/// </param>
		/// <param name="a">the analyzer to use
		/// </param>
		/// <param name="deletionPolicy">see <a href="#deletionPolicy">above</a>
		/// </param>
		/// <param name="mfl">whether or not to limit field lengths, value is in number of terms/tokens.  See <see cref="Lucene.Net.Index.IndexWriter.MaxFieldLength" />.
		/// </param>
		/// <param name="commit">which commit to open
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  LockObtainFailedException if another writer </throws>
		/// <summary>  has this index open (<c>write.lock</c> could not
		/// be obtained)
		/// </summary>
		/// <throws>  IOException if the directory cannot be read/written to, or </throws>
		/// <summary>  if it does not exist and <c>create</c> is
		/// <c>false</c> or if there is any other low-level
		/// IO error
		/// </summary>
		public IndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IndexCommit commit, IState state)
		{
			InitBlock();
			Init(d, a, false, deletionPolicy, mfl.Limit, null, commit, state);
		}
		
		private void  Init(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, int maxFieldLength, IndexingChain indexingChain, IndexCommit commit, IState state)
		{
			if (IndexReader.IndexExists(d, state))
			{
				Init(d, a, false, deletionPolicy, maxFieldLength, indexingChain, commit, state);
			}
			else
			{
				Init(d, a, true, deletionPolicy, maxFieldLength, indexingChain, commit, state);
			}
		}
		
		private void  Init(Directory d, Analyzer a, bool create, IndexDeletionPolicy deletionPolicy, int maxFieldLength, IndexingChain indexingChain, IndexCommit commit, IState state)
		{
			directory = d;
			analyzer = a;
			SetMessageID(defaultInfoStream);
			this.maxFieldLength = maxFieldLength;
			
			if (indexingChain == null)
				indexingChain = DocumentsWriter.DefaultIndexingChain;
			
			if (create)
			{
				// Clear the write lock in case it's leftover:
				directory.ClearLock(WRITE_LOCK_NAME);
			}
			
			Lock writeLock = directory.MakeLock(WRITE_LOCK_NAME);
			if (!writeLock.Obtain(writeLockTimeout))
			// obtain write lock
			{
				throw new LockObtainFailedException("Index locked for write: " + writeLock);
			}
			this.writeLock = writeLock; // save it

            bool success = false;
			try
			{
				if (create)
				{
					// Try to read first.  This is to allow create
					// against an index that's currently open for
					// searching.  In this case we write the next
					// segments_N file with no segments:
					bool doCommit;
					try
					{
						segmentInfos.Read(directory, state);
						segmentInfos.Clear();
						doCommit = false;
					}
					catch (System.IO.IOException)
					{
						// Likely this means it's a fresh directory
						doCommit = true;
					}
					
					if (doCommit)
					{
						// Only commit if there is no segments file 
                        // in this dir already.
						segmentInfos.Commit(directory, state);
                        synced.UnionWith(segmentInfos.Files(directory, true, state));
					}
					else
					{
						// Record that we have a change (zero out all
						// segments) pending:
						changeCount++;
					}
				}
				else
				{
					segmentInfos.Read(directory, state);
					
					if (commit != null)
					{
						// Swap out all segments, but, keep metadata in
						// SegmentInfos, like version & generation, to
						// preserve write-once.  This is important if
						// readers are open against the future commit
						// points.
						if (commit.Directory != directory)
							throw new System.ArgumentException("IndexCommit's directory doesn't match my directory");
						SegmentInfos oldInfos = new SegmentInfos();
						oldInfos.Read(directory, commit.SegmentsFileName, state);
						segmentInfos.Replace(oldInfos);
						changeCount++;
						if (infoStream != null)
							Message("init: loaded commit \"" + commit.SegmentsFileName + "\"");
					}
					
					// We assume that this segments_N was previously
					// properly sync'd:
                    synced.UnionWith(segmentInfos.Files(directory, true, state));
				}
				
				SetRollbackSegmentInfos(segmentInfos);

				docWriter = new DocumentsWriter(directory, this, indexingChain);
				docWriter.SetInfoStream(infoStream);
				docWriter.SetMaxFieldLength(maxFieldLength);
				
				// Default deleter (for backwards compatibility) is
				// KeepOnlyLastCommitDeleter:
				deleter = new IndexFileDeleter(directory, deletionPolicy == null?new KeepOnlyLastCommitDeletionPolicy():deletionPolicy, segmentInfos, infoStream, docWriter, synced, state);
				
				if (deleter.startingCommitDeleted)
				// Deletion policy deleted the "head" commit point.
				// We have to mark ourself as changed so that if we
				// are closed w/o any further changes we write a new
				// segments_N file.
					changeCount++;
				
				PushMaxBufferedDocs();
				
				if (infoStream != null)
				{
					Message("init: create=" + create);
					MessageState(state);
				}

                success = true;
			}
			finally
			{
                if (!success)
                {
                    if (infoStream != null)
                    {
                        Message("init: hit exception on init; releasing write lock");
                    }
                    try
                    {
                        writeLock.Release();
                    }
                    catch (Exception)
                    {
                        // don't mask the original exception
                    }
                    writeLock = null;
                }
			}
		}
		
		private void  SetRollbackSegmentInfos(SegmentInfos infos)
		{
			lock (this)
			{
				rollbackSegmentInfos = (SegmentInfos) infos.Clone();
				System.Diagnostics.Debug.Assert(!rollbackSegmentInfos.HasExternalSegments(directory));
				rollbackSegments = new HashMap<SegmentInfo, int?>();
				int size = rollbackSegmentInfos.Count;
				for (int i = 0; i < size; i++)
					rollbackSegments[rollbackSegmentInfos.Info(i)] = i;
			}
		}
		
		/// <summary> Expert: set the merge policy used by this writer.</summary>
		public virtual void  SetMergePolicy(MergePolicy mp)
		{
			EnsureOpen();
			if (mp == null)
				throw new System.NullReferenceException("MergePolicy must be non-null");
			
			if (mergePolicy != mp)
				mergePolicy.Close();
			mergePolicy = mp;
			PushMaxBufferedDocs();
			if (infoStream != null)
			{
				Message("setMergePolicy " + mp);
			}
		}

	    /// <summary> Expert: returns the current MergePolicy in use by this writer.</summary>
	    /// <seealso cref="SetMergePolicy">
	    /// </seealso>
	    public virtual MergePolicy MergePolicy
	    {
	        get
	        {
	            EnsureOpen();
	            return mergePolicy;
	        }
	    }

	    /// <summary> Expert: set the merge scheduler used by this writer.</summary>
		public virtual void  SetMergeScheduler(MergeScheduler mergeScheduler, IState state)
		{
			lock (this)
			{
				EnsureOpen();
				if (mergeScheduler == null)
					throw new System.NullReferenceException("MergeScheduler must be non-null");
				
				if (this.mergeScheduler != mergeScheduler)
				{
					FinishMerges(true, state);
					this.mergeScheduler.Close();
				}
				this.mergeScheduler = mergeScheduler;
				if (infoStream != null)
				{
					Message("setMergeScheduler " + mergeScheduler);
				}
			}
		}

	    /// <summary> Expert: returns the current MergePolicy in use by this
	    /// writer.
	    /// </summary>
	    /// <seealso cref="SetMergePolicy">
	    /// </seealso>
	    public virtual MergeScheduler MergeScheduler
	    {
	        get
	        {
	            EnsureOpen();
	            return mergeScheduler;
	        }
	    }

	    /// <summary> <p/>Gets or sets the largest segment (measured by document
        /// count) that may be merged with other segments.
        /// <p/> 
        /// Small values (e.g., less than 10,000) are best for
        /// interactive indexing, as this limits the length of
        /// pauses while indexing to a few seconds.  Larger values
        /// are best for batched indexing and speedier
        /// searches.
        /// <p/>
        /// The default value is <see cref="int.MaxValue" />.
        /// <p/>
        /// Note that this method is a convenience method: it
	    /// just calls mergePolicy.getMaxMergeDocs as long as
	    /// mergePolicy is an instance of <see cref="LogMergePolicy" />.
	    /// Otherwise an IllegalArgumentException is thrown.<p/>
        /// 
        /// The default merge policy (<see cref="LogByteSizeMergePolicy" />)
        /// also allows you to set this
        /// limit by net size (in MB) of the segment, using 
        /// <see cref="LogByteSizeMergePolicy.MaxMergeMB" />.<p/>
	    /// </summary>
	    /// <seealso cref="MaxMergeDocs">
	    /// </seealso>
	    public virtual int MaxMergeDocs
	    {
	        get { return LogMergePolicy.MaxMergeDocs; }
	        set { LogMergePolicy.MaxMergeDocs = value; }
	    }

	    /// <summary> The maximum number of terms that will be indexed for a single field in a
		/// document.  This limits the amount of memory required for indexing, so that
		/// collections with very large files will not crash the indexing process by
		/// running out of memory.  This setting refers to the number of running terms,
		/// not to the number of different terms.<p/>
		/// <strong>Note:</strong> this silently truncates large documents, excluding from the
		/// index all terms that occur further in the document.  If you know your source
		/// documents are large, be sure to set this value high enough to accomodate
		/// the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
		/// is your memory, but you should anticipate an OutOfMemoryError.<p/>
		/// By default, no more than <see cref="DEFAULT_MAX_FIELD_LENGTH" /> terms
		/// will be indexed for a field.
		/// </summary>
		public virtual void  SetMaxFieldLength(int maxFieldLength)
		{
			EnsureOpen();
			this.maxFieldLength = maxFieldLength;
			docWriter.SetMaxFieldLength(maxFieldLength);
			if (infoStream != null)
				Message("setMaxFieldLength " + maxFieldLength);
		}
		
		/// <summary> Returns the maximum number of terms that will be
		/// indexed for a single field in a document.
		/// </summary>
		/// <seealso cref="SetMaxFieldLength">
		/// </seealso>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual int GetMaxFieldLength()
		{
			EnsureOpen();
			return maxFieldLength;
		}

	    /// Gets or sets the termsIndexDivisor passed to any readers that
	    /// IndexWriter opens, for example when applying deletes
	    /// or creating a near-real-time reader in 
	    /// <see cref="GetReader()"/>.  Default value is 
	    /// <see cref="IndexReader.DEFAULT_TERMS_INDEX_DIVISOR"/>.
	    public int ReaderTermsIndexDivisor
	    {
	        get
	        {
	            EnsureOpen();
	            return readerTermsIndexDivisor;
	        }
	        set
	        {
	            EnsureOpen();
	            if (value <= 0)
	            {
	                throw new ArgumentException("divisor must be >= 1 (got " + value + ")");
	            }
	            readerTermsIndexDivisor = value;
	            if (infoStream != null)
	            {
	                Message("setReaderTermsIndexDivisor " + readerTermsIndexDivisor);
	            }
	        }
	    }

	    /// <summary>Determines the minimal number of documents required
		/// before the buffered in-memory documents are flushed as
		/// a new Segment.  Large values generally gives faster
		/// indexing.
		/// 
		/// <p/>When this is set, the writer will flush every
		/// maxBufferedDocs added documents.  Pass in <see cref="DISABLE_AUTO_FLUSH" />
		/// to prevent triggering a flush due
		/// to number of buffered documents.  Note that if flushing
		/// by RAM usage is also enabled, then the flush will be
		/// triggered by whichever comes first.<p/>
		/// 
		/// <p/>Disabled by default (writer flushes by RAM usage).<p/>
		/// 
		/// </summary>
		/// <throws>  IllegalArgumentException if maxBufferedDocs is </throws>
		/// <summary> enabled but smaller than 2, or it disables maxBufferedDocs
		/// when ramBufferSize is already disabled
		/// </summary>
		/// <seealso cref="SetRAMBufferSizeMB">
		/// </seealso>
		public virtual void  SetMaxBufferedDocs(int maxBufferedDocs)
		{
			EnsureOpen();
			if (maxBufferedDocs != DISABLE_AUTO_FLUSH && maxBufferedDocs < 2)
				throw new ArgumentException("maxBufferedDocs must at least be 2 when enabled");

			if (maxBufferedDocs == DISABLE_AUTO_FLUSH && (int)GetRAMBufferSizeMB() == DISABLE_AUTO_FLUSH)
				throw new ArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");

			docWriter.MaxBufferedDocs = maxBufferedDocs;
			PushMaxBufferedDocs();
			if (infoStream != null)
				Message("setMaxBufferedDocs " + maxBufferedDocs);
		}
		
		/// <summary> If we are flushing by doc count (not by RAM usage), and
		/// using LogDocMergePolicy then push maxBufferedDocs down
		/// as its minMergeDocs, to keep backwards compatibility.
		/// </summary>
		private void  PushMaxBufferedDocs()
		{
			if (docWriter.MaxBufferedDocs != DISABLE_AUTO_FLUSH)
			{
				MergePolicy mp = mergePolicy;
				if (mp is LogDocMergePolicy)
				{
					LogDocMergePolicy lmp = (LogDocMergePolicy) mp;
					int maxBufferedDocs = docWriter.MaxBufferedDocs;
					if (lmp.MinMergeDocs != maxBufferedDocs)
					{
						if (infoStream != null)
							Message("now push maxBufferedDocs " + maxBufferedDocs + " to LogDocMergePolicy");
						lmp.MinMergeDocs = maxBufferedDocs;
					}
				}
			}
		}
		
		/// <summary> Returns the number of buffered added documents that will
		/// trigger a flush if enabled.
		/// </summary>
		/// <seealso cref="SetMaxBufferedDocs">
		/// </seealso>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual int GetMaxBufferedDocs()
		{
			EnsureOpen();
			return docWriter.MaxBufferedDocs;
		}
		
		/// <summary>Determines the amount of RAM that may be used for
		/// buffering added documents and deletions before they are
		/// flushed to the Directory.  Generally for faster
		/// indexing performance it's best to flush by RAM usage
		/// instead of document count and use as large a RAM buffer
		/// as you can.
		/// 
		/// <p/>When this is set, the writer will flush whenever
		/// buffered documents and deletions use this much RAM.
		/// Pass in <see cref="DISABLE_AUTO_FLUSH" /> to prevent
		/// triggering a flush due to RAM usage.  Note that if
		/// flushing by document count is also enabled, then the
		/// flush will be triggered by whichever comes first.<p/>
		/// 
		/// <p/> <b>NOTE</b>: the account of RAM usage for pending
		/// deletions is only approximate.  Specifically, if you
		/// delete by Query, Lucene currently has no way to measure
		/// the RAM usage if individual Queries so the accounting
		/// will under-estimate and you should compensate by either
		/// calling commit() periodically yourself, or by using
		/// <see cref="SetMaxBufferedDeleteTerms" /> to flush by count
		/// instead of RAM usage (each buffered delete Query counts
		/// as one).
		/// 
		/// <p/>
		/// <b>NOTE</b>: because IndexWriter uses <c>int</c>s when managing its
		/// internal storage, the absolute maximum value for this setting is somewhat
		/// less than 2048 MB. The precise limit depends on various factors, such as
		/// how large your documents are, how many fields have norms, etc., so it's
		/// best to set this value comfortably under 2048.
		/// <p/>
		/// 
		/// <p/> The default value is <see cref="DEFAULT_RAM_BUFFER_SIZE_MB" />.<p/>
		/// 
		/// </summary>
		/// <throws>  IllegalArgumentException if ramBufferSize is </throws>
		/// <summary> enabled but non-positive, or it disables ramBufferSize
		/// when maxBufferedDocs is already disabled
		/// </summary>
		public virtual void  SetRAMBufferSizeMB(double mb)
		{
			if (mb > 2048.0)
			{
				throw new System.ArgumentException("ramBufferSize " + mb + " is too large; should be comfortably less than 2048");
			}
			if (mb != DISABLE_AUTO_FLUSH && mb <= 0.0)
				throw new System.ArgumentException("ramBufferSize should be > 0.0 MB when enabled");
			if (mb == DISABLE_AUTO_FLUSH && GetMaxBufferedDocs() == DISABLE_AUTO_FLUSH)
				throw new System.ArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");
			docWriter.SetRAMBufferSizeMB(mb);
			if (infoStream != null)
				Message("setRAMBufferSizeMB " + mb);
		}
		
		/// <summary> Returns the value set by <see cref="SetRAMBufferSizeMB" /> if enabled.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual double GetRAMBufferSizeMB()
		{
			return docWriter.GetRAMBufferSizeMB();
		}
		
		/// <summary> <p/>Determines the minimal number of delete terms required before the buffered
		/// in-memory delete terms are applied and flushed. If there are documents
		/// buffered in memory at the time, they are merged and a new segment is
		/// created.<p/>
		/// <p/>Disabled by default (writer flushes by RAM usage).<p/>
		/// 
		/// </summary>
		/// <throws>  IllegalArgumentException if maxBufferedDeleteTerms </throws>
		/// <summary> is enabled but smaller than 1
		/// </summary>
		/// <seealso cref="SetRAMBufferSizeMB">
		/// </seealso>
		public virtual void  SetMaxBufferedDeleteTerms(int maxBufferedDeleteTerms)
		{
			EnsureOpen();
			if (maxBufferedDeleteTerms != DISABLE_AUTO_FLUSH && maxBufferedDeleteTerms < 1)
				throw new System.ArgumentException("maxBufferedDeleteTerms must at least be 1 when enabled");
			docWriter.MaxBufferedDeleteTerms = maxBufferedDeleteTerms;
			if (infoStream != null)
				Message("setMaxBufferedDeleteTerms " + maxBufferedDeleteTerms);
		}
		
		/// <summary> Returns the number of buffered deleted terms that will
		/// trigger a flush if enabled.
		/// </summary>
		/// <seealso cref="SetMaxBufferedDeleteTerms">
		/// </seealso>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual int GetMaxBufferedDeleteTerms()
		{
			EnsureOpen();
			return docWriter.MaxBufferedDeleteTerms;
		}

	    /// <summary>Gets or sets the number of segments that are merged at
	    /// once and also controls the total number of segments
	    /// allowed to accumulate in the index.
	    /// <p/>Determines how often segment indices are merged by addDocument().  With
	    /// smaller values, less RAM is used while indexing, and searches on
	    /// unoptimized indices are faster, but indexing speed is slower.  With larger
	    /// values, more RAM is used during indexing, and while searches on unoptimized
	    /// indices are slower, indexing is faster.  Thus larger values (> 10) are best
	    /// for batch index creation, and smaller values (&lt; 10) for indices that are
	    /// interactively maintained.
	    /// 
	    /// <p/>Note that this method is a convenience method: it
	    /// just calls mergePolicy.setMergeFactor as long as
	    /// mergePolicy is an instance of <see cref="LogMergePolicy" />.
	    /// Otherwise an IllegalArgumentException is thrown.<p/>
	    /// 
	    /// <p/>This must never be less than 2.  The default value is 10.
	    /// </summary>
	    public virtual int MergeFactor
	    {
	        set { LogMergePolicy.MergeFactor = value; }
	        get { return LogMergePolicy.MergeFactor; }
	    }

	    /// <summary>Gets or sets the default info stream.
	    /// If non-null, this will be the default infoStream used
	    /// by a newly instantiated IndexWriter.
	    /// </summary>
	    /// <seealso cref="SetInfoStream">
	    /// </seealso>
	    public static StreamWriter DefaultInfoStream
	    {
	        set { IndexWriter.defaultInfoStream = value; }
	        get { return IndexWriter.defaultInfoStream; }
	    }

	    /// <summary>If non-null, information about merges, deletes and a
		/// message when maxFieldLength is reached will be printed
		/// to this.
		/// </summary>
		public virtual void  SetInfoStream(System.IO.StreamWriter infoStream, IState state)
		{
			EnsureOpen();
			SetMessageID(infoStream);
			docWriter.SetInfoStream(infoStream);
			deleter.SetInfoStream(infoStream);
			if (infoStream != null)
				MessageState(state);
		}
		
		private void  MessageState(IState state)
		{
		    Message("setInfoStream: dir=" + directory + 
                    " mergePolicy=" + mergePolicy + 
                    " mergeScheduler=" + mergeScheduler +
		            " ramBufferSizeMB=" + docWriter.GetRAMBufferSizeMB() + 
                    " maxBufferedDocs=" +  docWriter.MaxBufferedDocs +
                    " maxBuffereDeleteTerms=" + docWriter.MaxBufferedDeleteTerms +
		            " maxFieldLength=" + maxFieldLength + 
                    " index=" + SegString(state));
		}

	    /// <summary> Returns the current infoStream in use by this writer.</summary>
	    /// <seealso cref="SetInfoStream">
	    /// </seealso>
	    public virtual StreamWriter InfoStream
	    {
	        get
	        {
	            EnsureOpen();
	            return infoStream;
	        }
	    }

	    /// <summary>Returns true if verbosing is enabled (i.e., infoStream != null). </summary>
	    public virtual bool Verbose
	    {
	        get { return infoStream != null; }
	    }

	    /// <summary>Gets or sets allowed timeout when acquiring the write lock.</summary>
	    public virtual long WriteLockTimeout
	    {
	        get
	        {
	            EnsureOpen();
	            return writeLockTimeout;
	        }
	        set
	        {
	            EnsureOpen();
	            this.writeLockTimeout = value;
	        }
	    }

	    /// <summary> Gets or sets the default (for any instance of IndexWriter) maximum time to wait for a write lock (in
	    /// milliseconds).
	    /// </summary>
	    public static long DefaultWriteLockTimeout
	    {
	        set { IndexWriter.WRITE_LOCK_TIMEOUT = value; }
	        get { return IndexWriter.WRITE_LOCK_TIMEOUT; }
	    }

	    /// <summary> Commits all changes to an index and closes all
		/// associated files.  Note that this may be a costly
		/// operation, so, try to re-use a single writer instead of
		/// closing and opening a new one.  See <see cref="Commit()" /> for
		/// caveats about write caching done by some IO devices.
		/// 
		/// <p/> If an Exception is hit during close, eg due to disk
		/// full or some other reason, then both the on-disk index
		/// and the internal state of the IndexWriter instance will
		/// be consistent.  However, the close will not be complete
		/// even though part of it (flushing buffered documents)
		/// may have succeeded, so the write lock will still be
		/// held.<p/>
		/// 
		/// <p/> If you can correct the underlying cause (eg free up
		/// some disk space) then you can call close() again.
		/// Failing that, if you want to force the write lock to be
		/// released (dangerous, because you may then lose buffered
		/// docs in the IndexWriter instance) then you can do
		/// something like this:<p/>
		/// 
        /// <code>
		/// try {
		///     writer.close();
		/// } finally {
		///     if (IndexWriter.isLocked(directory)) {
		///         IndexWriter.unlock(directory);
		///     }
		/// }
        /// </code>
		/// 
		/// after which, you must be certain not to use the writer
		/// instance anymore.<p/>
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer, again.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		[Obsolete("Use Dispose() instead")]
		public void Close()
		{
		    Dispose(true);
		}

        /// <summary> Commits all changes to an index and closes all
        /// associated files.  Note that this may be a costly
        /// operation, so, try to re-use a single writer instead of
        /// closing and opening a new one.  See <see cref="Commit()" /> for
        /// caveats about write caching done by some IO devices.
        /// 
        /// <p/> If an Exception is hit during close, eg due to disk
        /// full or some other reason, then both the on-disk index
        /// and the internal state of the IndexWriter instance will
        /// be consistent.  However, the close will not be complete
        /// even though part of it (flushing buffered documents)
        /// may have succeeded, so the write lock will still be
        /// held.<p/>
        /// 
        /// <p/> If you can correct the underlying cause (eg free up
        /// some disk space) then you can call close() again.
        /// Failing that, if you want to force the write lock to be
        /// released (dangerous, because you may then lose buffered
        /// docs in the IndexWriter instance) then you can do
        /// something like this:<p/>
        /// 
        /// <code>
        /// try {
        ///     writer.close();
        /// } finally {
        ///     if (IndexWriter.isLocked(directory)) {
        ///         IndexWriter.unlock(directory);
        ///     }
        /// }
        /// </code>
        /// 
        /// after which, you must be certain not to use the writer
        /// instance anymore.<p/>
        /// 
        /// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
        /// you should immediately close the writer, again.  See <a
        /// href="#OOME">above</a> for details.<p/>
        /// 
        /// </summary>
        /// <throws>  CorruptIndexException if the index is corrupt </throws>
        /// <throws>  IOException if there is a low-level IO error </throws>
        public virtual void Dispose()
        {
            Dispose(true);
        }

        /// <summary> Closes the index with or without waiting for currently
        /// running merges to finish.  This is only meaningful when
        /// using a MergeScheduler that runs merges in background
        /// threads.
        /// 
        /// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
        /// you should immediately close the writer, again.  See <a
        /// href="#OOME">above</a> for details.<p/>
        /// 
        /// <p/><b>NOTE</b>: it is dangerous to always call
        /// close(false), especially when IndexWriter is not open
        /// for very long, because this can result in "merge
        /// starvation" whereby long merges will never have a
        /// chance to finish.  This will cause too many segments in
        /// your index over time.<p/>
        /// 
        /// </summary>
        /// <param name="waitForMerges">if true, this call will block
        /// until all merges complete; else, it will ask all
        /// running merges to abort, wait until those merges have
        /// finished (which should be at most a few seconds), and
        /// then return.
        /// </param>
        public virtual void Dispose(bool waitForMerges)
        {
            Dispose(true, waitForMerges, StateHolder.Current.Value);
        }

        protected virtual void Dispose(bool disposing, bool waitForMerges, IState state)
        {
            if (disposing)
            {
                // Ensure that only one thread actually gets to do the closing:
                if (ShouldClose())
                {
                    // If any methods have hit OutOfMemoryError, then abort
                    // on close, in case the internal state of IndexWriter
                    // or DocumentsWriter is corrupt
                    if (hitOOM)
                        RollbackInternal(state);
                    else
                        CloseInternal(waitForMerges, state);
                }
            }
        }
		
		/// <summary> Closes the index with or without waiting for currently
		/// running merges to finish.  This is only meaningful when
		/// using a MergeScheduler that runs merges in background
		/// threads.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer, again.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// <p/><b>NOTE</b>: it is dangerous to always call
		/// close(false), especially when IndexWriter is not open
		/// for very long, because this can result in "merge
		/// starvation" whereby long merges will never have a
		/// chance to finish.  This will cause too many segments in
		/// your index over time.<p/>
		/// 
		/// </summary>
		/// <param name="waitForMerges">if true, this call will block
		/// until all merges complete; else, it will ask all
		/// running merges to abort, wait until those merges have
		/// finished (which should be at most a few seconds), and
		/// then return.
		/// </param>
		[Obsolete("Use Dispose(bool) instead")]
		public virtual void Close(bool waitForMerges)
		{
		    Dispose(waitForMerges);
		}
		
		// Returns true if this thread should attempt to close, or
		// false if IndexWriter is now closed; else, waits until
		// another thread finishes closing
		private bool ShouldClose()
		{
			lock (this)
			{
				while (true)
				{
					if (!closed)
					{
						if (!closing)
						{
							closing = true;
							return true;
						}
						else
						{
							// Another thread is presently trying to close;
							// wait until it finishes one way (closes
							// successfully) or another (fails to close)
							DoWait();
						}
					}
					else
						return false;
				}
			}
		}
		
		private void CloseInternal(bool waitForMerges, IState state)
		{
			
			docWriter.PauseAllThreads();
			
			try
			{
				if (infoStream != null)
					Message("now flush at close");
				
				docWriter.Dispose();
				
				// Only allow a new merge to be triggered if we are
				// going to wait for merges:
				if (!hitOOM)
				{
					Flush(waitForMerges, true, true, state);
				}
				
				if (waitForMerges)
				// Give merge scheduler last chance to run, in case
				// any pending merges are waiting:
					mergeScheduler.Merge(this, state);
				
				mergePolicy.Close();
				
				FinishMerges(waitForMerges, state);
				stopMerges = true;
				
				mergeScheduler.Close();
				
				if (infoStream != null)
					Message("now call final commit()");
				
				if (!hitOOM)
				{
					Commit(0, state);
				}
				
				if (infoStream != null)
					Message("at close: " + SegString(state));
				
				lock (this)
				{
					readerPool.Dispose();
					docWriter = null;
					deleter.Dispose();
				}
				
				if (writeLock != null)
				{
					writeLock.Release(); // release write lock
					writeLock = null;
				}
				lock (this)
				{
					closed = true;
				}
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "closeInternal");
			}
			finally
			{
				lock (this)
				{
					closing = false;
					System.Threading.Monitor.PulseAll(this);
					if (!closed)
					{
						if (docWriter != null)
							docWriter.ResumeAllThreads();
						if (infoStream != null)
							Message("hit exception while closing");
					}
				}
			}
		}
		
		/// <summary>Tells the docWriter to close its currently open shared
		/// doc stores (stored fields &amp; vectors files).
		/// Return value specifices whether new doc store files are compound or not.
		/// </summary>
		private bool FlushDocStores(IState state)
		{
			lock (this)
			{
                if (infoStream != null)
                {
                    Message("flushDocStores segment=" + docWriter.DocStoreSegment);
                }

				bool useCompoundDocStore = false;
                if (infoStream != null)
                {
                    Message("closeDocStores segment=" + docWriter.DocStoreSegment);
                }

				System.String docStoreSegment;
				
				bool success = false;
				try
				{
					docStoreSegment = docWriter.CloseDocStore(state);
					success = true;
				}
				finally
				{
					if (!success && infoStream != null)
					{
						Message("hit exception closing doc store segment");
					}
				}

                if (infoStream != null)
                {
                    Message("flushDocStores files=" + docWriter.ClosedFiles());
                }

				useCompoundDocStore = mergePolicy.UseCompoundDocStore(segmentInfos);
				
				if (useCompoundDocStore && docStoreSegment != null && docWriter.ClosedFiles().Count != 0)
				{
					// Now build compound doc store file
					
					if (infoStream != null)
					{
						Message("create compound file " + docStoreSegment + "." + IndexFileNames.COMPOUND_FILE_STORE_EXTENSION);
					}
					
					success = false;
					
					int numSegments = segmentInfos.Count;
					System.String compoundFileName = docStoreSegment + "." + IndexFileNames.COMPOUND_FILE_STORE_EXTENSION;
					
					try
					{
						CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, compoundFileName);
						foreach(string file in docWriter.closedFiles)
						{
							cfsWriter.AddFile(file);
						}
						
						// Perform the merge
						cfsWriter.Close();
						success = true;
					}
					finally
					{
						if (!success)
						{
							if (infoStream != null)
								Message("hit exception building compound file doc store for segment " + docStoreSegment);
							deleter.DeleteFile(compoundFileName, state);
                            docWriter.Abort();
						}
					}
					
					for (int i = 0; i < numSegments; i++)
					{
						SegmentInfo si = segmentInfos.Info(i);
						if (si.DocStoreOffset != - 1 && si.DocStoreSegment.Equals(docStoreSegment))
							si.DocStoreIsCompoundFile = true;
					}
					
					Checkpoint(state);
					
					// In case the files we just merged into a CFS were
					// not previously checkpointed:
					deleter.DeleteNewFiles(docWriter.ClosedFiles(), state);
				}
				
				return useCompoundDocStore;
			}
		}

	    /// <summary>Returns the Directory used by this index. </summary>
	    public virtual Directory Directory
	    {
	        get
	        {
	            // Pass false because the flush during closing calls getDirectory
	            EnsureOpen(false);
	            return directory;
	        }
	    }

	    /// <summary>Returns the analyzer used by this index. </summary>
	    public virtual Analyzer Analyzer
	    {
	        get
	        {
	            EnsureOpen();
	            return analyzer;
	        }
	    }

	    /// <summary>Returns total number of docs in this index, including
	    /// docs not yet flushed (still in the RAM buffer),
	    /// not counting deletions.
	    /// </summary>
	    /// <seealso cref="NumDocs">
	    /// </seealso>
	    public virtual int MaxDoc()
	    {
	        lock (this)
	        {
	            int count;
	            if (docWriter != null)
	                count = docWriter.NumDocsInRAM;
	            else
	                count = 0;

	            for (int i = 0; i < segmentInfos.Count; i++)
	                count += segmentInfos.Info(i).docCount;
	            return count;
	        }
	    }

	    /// <summary>Returns total number of docs in this index, including
		/// docs not yet flushed (still in the RAM buffer), and
		/// including deletions.  <b>NOTE:</b> buffered deletions
		/// are not counted.  If you really need these to be
		/// counted you should call <see cref="Commit()" /> first.
		/// </summary>
		/// <seealso cref="NumDocs">
		/// </seealso>
		public virtual int NumDocs(IState state)
		{
			lock (this)
			{
				int count;
				if (docWriter != null)
					count = docWriter.NumDocsInRAM;
				else
					count = 0;
				
				for (int i = 0; i < segmentInfos.Count; i++)
				{
					SegmentInfo info = segmentInfos.Info(i);
					count += info.docCount - info.GetDelCount(state);
				}
				return count;
			}
		}
		
		public virtual bool HasDeletions(IState state)
		{
			lock (this)
			{
				EnsureOpen();
				if (docWriter.HasDeletes())
					return true;
				for (int i = 0; i < segmentInfos.Count; i++)
					if (segmentInfos.Info(i).HasDeletions(state))
						return true;
				return false;
			}
		}
		
		/// <summary> The maximum number of terms that will be indexed for a single field in a
		/// document.  This limits the amount of memory required for indexing, so that
		/// collections with very large files will not crash the indexing process by
		/// running out of memory.<p/>
		/// Note that this effectively truncates large documents, excluding from the
		/// index terms that occur further in the document.  If you know your source
		/// documents are large, be sure to set this value high enough to accomodate
		/// the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
		/// is your memory, but you should anticipate an OutOfMemoryError.<p/>
		/// By default, no more than 10,000 terms will be indexed for a field.
		/// 
		/// </summary>
		/// <seealso cref="MaxFieldLength">
		/// </seealso>
		private int maxFieldLength;
		
		/// <summary> Adds a document to this index.  If the document contains more than
		/// <see cref="SetMaxFieldLength(int)" /> terms for a given field, the remainder are
		/// discarded.
		/// 
		/// <p/> Note that if an Exception is hit (for example disk full)
		/// then the index will be consistent, but this document
		/// may not have been added.  Furthermore, it's possible
		/// the index will have one segment in non-compound format
		/// even when using compound files (when a merge has
		/// partially succeeded).<p/>
		/// 
		/// <p/> This method periodically flushes pending documents
		/// to the Directory (see <a href="#flush">above</a>), and
		/// also periodically triggers segment merges in the index
		/// according to the <see cref="MergePolicy" /> in use.<p/>
		/// 
		/// <p/>Merges temporarily consume space in the
		/// directory. The amount of space required is up to 1X the
		/// size of all segments being merged, when no
		/// readers/searchers are open against the index, and up to
		/// 2X the size of all segments being merged when
		/// readers/searchers are open against the index (see
		/// <see cref="Optimize()" /> for details). The sequence of
		/// primitive merge operations performed is governed by the
		/// merge policy.
		/// 
		/// <p/>Note that each term in the document can be no longer
		/// than 16383 characters, otherwise an
		/// IllegalArgumentException will be thrown.<p/>
		/// 
		/// <p/>Note that it's possible to create an invalid Unicode
		/// string in java if a UTF16 surrogate pair is malformed.
		/// In this case, the invalid characters are silently
		/// replaced with the Unicode replacement character
		/// U+FFFD.<p/>
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void AddDocument(Document doc, IState state)
		{
			AddDocument(doc, analyzer, state);
		}
		
		/// <summary> Adds a document to this index, using the provided analyzer instead of the
		/// value of <see cref="Analyzer" />.  If the document contains more than
		/// <see cref="SetMaxFieldLength(int)" /> terms for a given field, the remainder are
		/// discarded.
		/// 
		/// <p/>See <see cref="AddDocument(Document)" /> for details on
		/// index and IndexWriter state after an Exception, and
		/// flushing/merging temporary free space requirements.<p/>
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void AddDocument(Document doc, Analyzer analyzer, IState state)
		{
			EnsureOpen();
			bool doFlush = false;
			bool success = false;
			try
			{
				try
				{
					doFlush = docWriter.AddDocument(doc, analyzer, state);
					success = true;
				}
				finally
				{
					if (!success)
					{
						
						if (infoStream != null)
							Message("hit exception adding document");
						
						lock (this)
						{
							// If docWriter has some aborted files that were
							// never incref'd, then we clean them up here
							if (docWriter != null)
							{
                                ICollection<string> files = docWriter.AbortedFiles();
								if (files != null)
									deleter.DeleteNewFiles(files, state);
							}
						}
					}
				}
				if (doFlush)
					Flush(true, false, false, state);
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "addDocument");
			}
		}
		
		/// <summary> Deletes the document(s) containing <c>term</c>.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="term">the term to identify the documents to be deleted
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  DeleteDocuments(Term term, IState state)
		{
			EnsureOpen();
			try
			{
				bool doFlush = docWriter.BufferDeleteTerm(term);
				if (doFlush)
					Flush(true, false, false, state);
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "deleteDocuments(Term)");
			}
		}
		
		/// <summary> Deletes the document(s) containing any of the
		/// terms. All deletes are flushed at the same time.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="terms">array of terms to identify the documents
		/// to be deleted
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  DeleteDocuments(IState state, params Term[] terms)
		{
			EnsureOpen();
			try
			{
				bool doFlush = docWriter.BufferDeleteTerms(terms);
				if (doFlush)
					Flush(true, false, false, state);
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "deleteDocuments(params Term[])");
			}
		}
		
		/// <summary> Deletes the document(s) matching the provided query.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="query">the query to identify the documents to be deleted
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  DeleteDocuments(IState state, Query query)
		{
			EnsureOpen();
			bool doFlush = docWriter.BufferDeleteQuery(query);
			if (doFlush)
				Flush(true, false, false, state);
		}
		
		/// <summary> Deletes the document(s) matching any of the provided queries.
		/// All deletes are flushed at the same time.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="queries">array of queries to identify the documents
		/// to be deleted
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  DeleteDocuments(IState state, params Query[] queries)
		{
			EnsureOpen();
			bool doFlush = docWriter.BufferDeleteQueries(queries);
			if (doFlush)
				Flush(true, false, false, state);
		}
		
		/// <summary> Updates a document by first deleting the document(s)
		/// containing <c>term</c> and then adding the new
		/// document.  The delete and then add are atomic as seen
		/// by a reader on the same index (flush may happen only after
		/// the add).
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="term">the term to identify the document(s) to be
		/// deleted
		/// </param>
		/// <param name="doc">the document to be added
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  UpdateDocument(Term term, Document doc, IState state)
		{
			EnsureOpen();
			UpdateDocument(term, doc, Analyzer, state);
		}
		
		/// <summary> Updates a document by first deleting the document(s)
		/// containing <c>term</c> and then adding the new
		/// document.  The delete and then add are atomic as seen
		/// by a reader on the same index (flush may happen only after
		/// the add).
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="term">the term to identify the document(s) to be
		/// deleted
		/// </param>
		/// <param name="doc">the document to be added
		/// </param>
		/// <param name="analyzer">the analyzer to use when analyzing the document
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  UpdateDocument(Term term, Document doc, Analyzer analyzer, IState state)
		{
			EnsureOpen();
			try
			{
				bool doFlush = false;
				bool success = false;
				try
				{
					doFlush = docWriter.UpdateDocument(term, doc, analyzer, state);
					success = true;
				}
				finally
				{
					if (!success)
					{
						
						if (infoStream != null)
							Message("hit exception updating document");
						
						lock (this)
						{
							// If docWriter has some aborted files that were
							// never incref'd, then we clean them up here
                            ICollection<string> files = docWriter.AbortedFiles();
							if (files != null)
								deleter.DeleteNewFiles(files, state);
						}
					}
				}
				if (doFlush)
					Flush(true, false, false, state);
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "updateDocument");
			}
		}
		
		// for test purpose
		internal int GetSegmentCount()
		{
			lock (this)
			{
				return segmentInfos.Count;
			}
		}
		
		// for test purpose
		internal int GetNumBufferedDocuments()
		{
			lock (this)
			{
				return docWriter.NumDocsInRAM;
			}
		}
		
		// for test purpose
		public /*internal*/ int GetDocCount(int i)
		{
			lock (this)
			{
				if (i >= 0 && i < segmentInfos.Count)
				{
					return segmentInfos.Info(i).docCount;
				}
				else
				{
					return - 1;
				}
			}
		}
		
		// for test purpose
		internal int GetFlushCount()
		{
			lock (this)
			{
				return flushCount;
			}
		}
		
		// for test purpose
		internal int GetFlushDeletesCount()
		{
			lock (this)
			{
				return flushDeletesCount;
			}
		}
		
		internal System.String NewSegmentName()
		{
			// Cannot synchronize on IndexWriter because that causes
			// deadlock
			lock (segmentInfos)
			{
				// Important to increment changeCount so that the
				// segmentInfos is written on close.  Otherwise we
				// could close, re-open and re-return the same segment
				// name that was previously returned which can cause
				// problems at least with ConcurrentMergeScheduler.
				changeCount++;
				return "_" + Number.ToString(segmentInfos.counter++);
			}
		}
		
		/// <summary>If non-null, information about merges will be printed to this.</summary>
		private System.IO.StreamWriter infoStream = null;
		private static System.IO.StreamWriter defaultInfoStream = null;
		
		/// <summary> Requests an "optimize" operation on an index, priming the index
		/// for the fastest available search. Traditionally this has meant
		/// merging all segments into a single segment as is done in the
		/// default merge policy, but individaul merge policies may implement
		/// optimize in different ways.
		/// 
		/// <p/>It is recommended that this method be called upon completion of indexing.  In
		/// environments with frequent updates, optimize is best done during low volume times, if at all. 
		/// 
		/// <p/>
		/// <p/>See http://www.gossamer-threads.com/lists/lucene/java-dev/47895 for more discussion. <p/>
		/// 
		/// <p/>Note that optimize requires 2X the index size free
		/// space in your Directory (3X if you're using compound
        /// file format).  For example, if your index
		/// size is 10 MB then you need 20 MB free for optimize to
        /// complete (30 MB if you're using compound fiel format).<p/>
		/// 
		/// <p/>If some but not all readers re-open while an
		/// optimize is underway, this will cause > 2X temporary
		/// space to be consumed as those new readers will then
		/// hold open the partially optimized segments at that
		/// time.  It is best not to re-open readers while optimize
		/// is running.<p/>
		/// 
		/// <p/>The actual temporary usage could be much less than
		/// these figures (it depends on many factors).<p/>
		/// 
		/// <p/>In general, once the optimize completes, the total size of the
		/// index will be less than the size of the starting index.
		/// It could be quite a bit smaller (if there were many
		/// pending deletes) or just slightly smaller.<p/>
		/// 
		/// <p/>If an Exception is hit during optimize(), for example
		/// due to disk full, the index will not be corrupt and no
		/// documents will have been lost.  However, it may have
		/// been partially optimized (some segments were merged but
		/// not all), and it's possible that one of the segments in
		/// the index will be in non-compound format even when
		/// using compound file format.  This will occur when the
		/// Exception is hit during conversion of the segment into
		/// compound format.<p/>
		/// 
		/// <p/>This call will optimize those segments present in
		/// the index when the call started.  If other threads are
		/// still adding documents and flushing segments, those
		/// newly created segments will not be optimized unless you
		/// call optimize again.<p/>
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		/// <seealso cref="Net.Index.LogMergePolicy.FindMergesForOptimize">
		/// </seealso>
		public virtual void  Optimize(IState state, CancellationToken token = default)
		{
			Optimize(true, state, token);
		}
		
		public virtual void  Optimize(StreamWriter writer, IState state, CancellationToken token)
		{
			infoStream = writer;
			Optimize(true, state, token);
		}

        /// <summary> Optimize the index down to &lt;= maxNumSegments.  If
		/// maxNumSegments==1 then this is the same as <see cref="Optimize()" />
		///.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="maxNumSegments">maximum number of segments left
		/// in the index after optimization finishes
		/// </param>
		public virtual void  Optimize(int maxNumSegments, IState state, CancellationToken token)
		{
			Optimize(maxNumSegments, true, state, token);
		}
		
		/// <summary>Just like <see cref="Optimize()" />, except you can specify
		/// whether the call should block until the optimize
		/// completes.  This is only meaningful with a
		/// <see cref="MergeScheduler" /> that is able to run merges in
		/// background threads.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// </summary>
		public virtual void  Optimize(bool doWait, IState state, CancellationToken token)
		{
			Optimize(1, doWait, state, token);
		}

		/// <summary>Just like <see cref="Optimize(int)" />, except you can
		/// specify whether the call should block until the
		/// optimize completes.  This is only meaningful with a
		/// <see cref="MergeScheduler" /> that is able to run merges in
		/// background threads.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// </summary>
		public virtual void Optimize(int maxNumSegments, bool doWait, IState state, CancellationToken token)
		{
			lock (this)
				_optimizeScope = new OptimizeScope(token);
			
			using (_optimizeScope)
			{
				EnsureOpen();

				if (maxNumSegments < 1)
					throw new System.ArgumentException("maxNumSegments must be >= 1; got " + maxNumSegments);

				if (infoStream != null)
					Message("optimize: index now " + SegString(state));

				Flush(true, false, true, state);

				lock (this)
				{
					ResetMergeExceptions();
					segmentsToOptimize = Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet<SegmentInfo>();
					optimizeMaxNumSegments = maxNumSegments;
					int numSegments = segmentInfos.Count;
					for (int i = 0; i < numSegments; i++)
						segmentsToOptimize.Add(segmentInfos.Info(i));

					// Now mark all pending & running merges as optimize
					// merge:
					foreach (MergePolicy.OneMerge merge in pendingMerges)
					{
						merge.optimize = true;
						merge.maxNumSegmentsOptimize = maxNumSegments;
					}

					foreach (MergePolicy.OneMerge merge in runningMerges)
					{
						merge.optimize = true;
						merge.maxNumSegmentsOptimize = maxNumSegments;
					}
				}

				MaybeMerge(maxNumSegments, true, state);

				if (doWait)
				{
					lock (this)
					{
						while (true)
						{

							if (hitOOM)
							{
								throw new System.SystemException("this writer hit an OutOfMemoryError; cannot complete optimize");
							}

							if (mergeExceptions.Count > 0)
							{
								// Forward any exceptions in background merge
								// threads to the current thread:
								int size = mergeExceptions.Count;
								for (int i = 0; i < size; i++)
								{
									MergePolicy.OneMerge merge = mergeExceptions[i];
									if (merge.optimize)
									{
										System.IO.IOException err;
										System.Exception t = merge.GetException();
										if (t != null)
											err = new System.IO.IOException("background merge hit exception: " + merge.SegString(directory, state), t);
										else
											err = new System.IO.IOException("background merge hit exception: " + merge.SegString(directory, state));
										throw err;
									}
								}
							}

							if (OptimizeMergesPending())
							{
								if (token.IsCancellationRequested)
								{
									foreach (var merge in runningMerges)
										merge.Abort();
									foreach (var merge in pendingMerges)
										merge.Abort();
									token.ThrowIfCancellationRequested();
								}

								DoWait();
							}
							else
								break;
						}
					}

					// If close is called while we are still
					// running, throw an exception so the calling
					// thread will know the optimize did not
					// complete
					EnsureOpen();
				}

				// NOTE: in the ConcurrentMergeScheduler case, when
				// doWait is false, we can return immediately while
				// background threads accomplish the optimization
			}
		}

		/// <summary>Returns true if any merges in pendingMerges or
		/// runningMerges are optimization merges. 
		/// </summary>
		public bool OptimizeMergesPending()
		{
			lock (this)
			{
                foreach (MergePolicy.OneMerge merge in pendingMerges)
                {
                    if (merge.optimize) return true;
                }

                foreach(MergePolicy.OneMerge merge in runningMerges)
                {
                    if (merge.optimize) return true;
                }
				
				return false;
			}
		}
		
		/// <summary>Just like <see cref="ExpungeDeletes()" />, except you can
		/// specify whether the call should block until the
		/// operation completes.  This is only meaningful with a
		/// <see cref="MergeScheduler" /> that is able to run merges in
		/// background threads.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// </summary>
		public virtual void  ExpungeDeletes(bool doWait, IState state)
		{
			EnsureOpen();
			
			if (infoStream != null)
				Message("expungeDeletes: index now " + SegString(state));
			
			MergePolicy.MergeSpecification spec;
			
			lock (this)
			{
				spec = mergePolicy.FindMergesToExpungeDeletes(segmentInfos, state);
				if (spec != null)
				{
					int numMerges = spec.merges.Count;
					for (int i = 0; i < numMerges; i++)
						RegisterMerge(spec.merges[i], state);
				}
			}
			
			mergeScheduler.Merge(this, state);
			
			if (spec != null && doWait)
			{
				int numMerges = spec.merges.Count;
				lock (this)
				{
					bool running = true;
					while (running)
					{
						
						if (hitOOM)
						{
							throw new System.SystemException("this writer hit an OutOfMemoryError; cannot complete expungeDeletes");
						}
						
						// Check each merge that MergePolicy asked us to
						// do, to see if any of them are still running and
						// if any of them have hit an exception.
						running = false;
						for (int i = 0; i < numMerges; i++)
						{
							MergePolicy.OneMerge merge = spec.merges[i];
							if (pendingMerges.Contains(merge) || runningMerges.Contains(merge))
								running = true;
							System.Exception t = merge.GetException();
							if (t != null)
							{
								System.IO.IOException ioe = new System.IO.IOException("background merge hit exception: " + merge.SegString(directory, state), t);
								throw ioe;
							}
						}
						
						// If any of our merges are still running, wait:
						if (running)
							DoWait();
					}
				}
			}
			
			// NOTE: in the ConcurrentMergeScheduler case, when
			// doWait is false, we can return immediately while
			// background threads accomplish the optimization
		}
		
		
		/// <summary>Expunges all deletes from the index.  When an index
		/// has many document deletions (or updates to existing
		/// documents), it's best to either call optimize or
		/// expungeDeletes to remove all unused data in the index
		/// associated with the deleted documents.  To see how
		/// many deletions you have pending in your index, call
		/// <see cref="IndexReader.NumDeletedDocs" />
		/// This saves disk space and memory usage while
		/// searching.  expungeDeletes should be somewhat faster
		/// than optimize since it does not insist on reducing the
		/// index to a single segment (though, this depends on the
		/// <see cref="MergePolicy" />; see <see cref="Net.Index.MergePolicy.FindMergesToExpungeDeletes" />.). Note that
		/// this call does not first commit any buffered
		/// documents, so you must do so yourself if necessary.
		/// See also <seealso cref="ExpungeDeletes(bool)" />
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// </summary>
		public virtual void  ExpungeDeletes(IState state)
		{
			ExpungeDeletes(true, state);
		}
		
		/// <summary> Expert: asks the mergePolicy whether any merges are
		/// necessary now and if so, runs the requested merges and
		/// then iterate (test again if merges are needed) until no
		/// more merges are returned by the mergePolicy.
		/// 
		/// Explicit calls to maybeMerge() are usually not
		/// necessary. The most common case is when merge policy
		/// parameters have changed.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// </summary>
		public void  MaybeMerge(IState state)
		{
			MaybeMerge(false, state);
		}
		
		private void  MaybeMerge(bool optimize, IState state)
		{
			MaybeMerge(1, optimize, state);
		}
		
		private void  MaybeMerge(int maxNumSegmentsOptimize, bool optimize, IState state)
		{
			UpdatePendingMerges(maxNumSegmentsOptimize, optimize, state);
			mergeScheduler.Merge(this, state);
		}
		
		private void  UpdatePendingMerges(int maxNumSegmentsOptimize, bool optimize, IState state)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(!optimize || maxNumSegmentsOptimize > 0);

                if (stopMerges)
                {
                    return;
                }
				
				// Do not start new merges if we've hit OOME
				if (hitOOM)
				{
					return ;
				}
				
				MergePolicy.MergeSpecification spec;
                if (optimize)
                {
                    spec = mergePolicy.FindMergesForOptimize(segmentInfos, maxNumSegmentsOptimize, segmentsToOptimize, state);

                    if (spec != null)
                    {
                        int numMerges = spec.merges.Count;
                        for (int i = 0; i < numMerges; i++)
                        {
                            MergePolicy.OneMerge merge = spec.merges[i];
                            merge.optimize = true;
                            merge.maxNumSegmentsOptimize = maxNumSegmentsOptimize;
                        }
                    }
                }
                else
                {
                    spec = mergePolicy.FindMerges(segmentInfos, state);
                }

			    if (spec != null)
				{
					int numMerges = spec.merges.Count;
					for (int i = 0; i < numMerges; i++)
						RegisterMerge(spec.merges[i], state);
				}
			}
		}
		
		/// <summary>Expert: the <see cref="MergeScheduler" /> calls this method
		/// to retrieve the next merge requested by the
		/// MergePolicy 
		/// </summary>
		public virtual MergePolicy.OneMerge GetNextMerge()
		{
			lock (this)
			{
				if (pendingMerges.Count == 0)
					return null;
				else
				{
                    // Advance the merge from pending to running
                    MergePolicy.OneMerge merge = pendingMerges.First.Value;
                    pendingMerges.RemoveFirst();
                    runningMerges.Add(merge);
                    return merge;
				}
			}
		}
		
		/// <summary>Like getNextMerge() except only returns a merge if it's
		/// external. 
		/// </summary>
		private MergePolicy.OneMerge GetNextExternalMerge()
		{
			lock (this)
			{
				if (pendingMerges.Count == 0)
					return null;
				else
				{
                    var it = pendingMerges.GetEnumerator();
					while (it.MoveNext())
					{
                        MergePolicy.OneMerge merge = it.Current;
						if (merge.isExternal)
						{
							// Advance the merge from pending to running
                            pendingMerges.Remove(merge);  // {{Aroush-2.9}} From Mike Garski: this is an O(n) op... is that an issue?
                            runningMerges.Add(merge);
							return merge;
						}
					}
					
					// All existing merges do not involve external segments
					return null;
				}
			}
		}
		
		/*
		* Begin a transaction.  During a transaction, any segment
		* merges that happen (or ram segments flushed) will not
		* write a new segments file and will not remove any files
		* that were present at the start of the transaction.  You
		* must make a matched (try/finally) call to
		* commitTransaction() or rollbackTransaction() to finish
		* the transaction.
		*
		* Note that buffered documents and delete terms are not handled
		* within the transactions, so they must be flushed before the
		* transaction is started.
		*/
		private void  StartTransaction(bool haveReadLock, IState state)
		{
			lock (this)
			{
				
				bool success = false;
				try
				{
					if (infoStream != null)
						Message("now start transaction");
					
					System.Diagnostics.Debug.Assert(docWriter.GetNumBufferedDeleteTerms() == 0 , 
						"calling startTransaction with buffered delete terms not supported: numBufferedDeleteTerms=" + docWriter.GetNumBufferedDeleteTerms());
					System.Diagnostics.Debug.Assert(docWriter.NumDocsInRAM == 0 , 
						"calling startTransaction with buffered documents not supported: numDocsInRAM=" + docWriter.NumDocsInRAM);
					
					EnsureOpen();
					
					// If a transaction is trying to roll back (because
					// addIndexes hit an exception) then wait here until
					// that's done:
					lock (this)
					{
						while (stopMerges)
							DoWait();
					}
					success = true;
				}
				finally
				{
					// Release the write lock if our caller held it, on
					// hitting an exception
					if (!success && haveReadLock)
						ReleaseRead();
				}
				
				if (haveReadLock)
				{
					UpgradeReadToWrite();
				}
				else
				{
					AcquireWrite();
				}
				
				success = false;
				try
				{
					localRollbackSegmentInfos = (SegmentInfos) segmentInfos.Clone();
					
					System.Diagnostics.Debug.Assert(!HasExternalSegments());
					
					localFlushedDocCount = docWriter.GetFlushedDocCount();

                    // Remove the incRef we did in startTransaction:
					deleter.IncRef(segmentInfos, false, state);
					
					success = true;
				}
				finally
				{
					if (!success)
						FinishAddIndexes();
				}
			}
		}
		
		/*
		* Rolls back the transaction and restores state to where
		* we were at the start.
		*/
		private void  RollbackTransaction(IState state)
		{
			lock (this)
			{
				
				if (infoStream != null)
					Message("now rollback transaction");
				
				if (docWriter != null)
				{
					docWriter.SetFlushedDocCount(localFlushedDocCount);
				}
				
				// Must finish merges before rolling back segmentInfos
				// so merges don't hit exceptions on trying to commit
				// themselves, don't get files deleted out from under
				// them, etc:
				FinishMerges(false, state);
				
				// Keep the same segmentInfos instance but replace all
				// of its SegmentInfo instances.  This is so the next
				// attempt to commit using this instance of IndexWriter
				// will always write to a new generation ("write once").
				segmentInfos.Clear();
				segmentInfos.AddRange(localRollbackSegmentInfos);
				localRollbackSegmentInfos = null;
				
				// This must come after we rollback segmentInfos, so
				// that if a commit() kicks off it does not see the
				// segmentInfos with external segments
				FinishAddIndexes();
				
				// Ask deleter to locate unreferenced files we had
				// created & remove them:
				deleter.Checkpoint(segmentInfos, false, state);

                // Remove the incRef we did in startTransaction:
				deleter.DecRef(segmentInfos, state);
				
				// Also ask deleter to remove any newly created files
				// that were never incref'd; this "garbage" is created
				// when a merge kicks off but aborts part way through
				// before it had a chance to incRef the files it had
				// partially created
				deleter.Refresh(state);
				
				System.Threading.Monitor.PulseAll(this);
				
				System.Diagnostics.Debug.Assert(!HasExternalSegments());
			}
		}
		
		/*
		* Commits the transaction.  This will write the new
		* segments file and remove and pending deletions we have
		* accumulated during the transaction
		*/
		private void  CommitTransaction(IState state)
		{
			lock (this)
			{
				
				if (infoStream != null)
					Message("now commit transaction");
				
				// Give deleter a chance to remove files now:
				Checkpoint(state);
				
				// Remove the incRef we did in startTransaction.
                deleter.DecRef(localRollbackSegmentInfos, state);
				
				localRollbackSegmentInfos = null;
				
				System.Diagnostics.Debug.Assert(!HasExternalSegments());
				
				FinishAddIndexes();
			}
		}
		
		/// <summary> Close the <c>IndexWriter</c> without committing
		/// any changes that have occurred since the last commit
		/// (or since it was opened, if commit hasn't been called).
		/// This removes any temporary files that had been created,
		/// after which the state of the index will be the same as
		/// it was when commit() was last called or when this
		/// writer was first opened.  This also clears a previous 
		/// call to <see cref="PrepareCommit()" />.
		/// </summary>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  Rollback(IState state)
		{
			EnsureOpen();
			
			// Ensure that only one thread actually gets to do the closing:
			if (ShouldClose())
				RollbackInternal(state);
		}
		
		private void  RollbackInternal(IState state)
		{
			
			bool success = false;

            if (infoStream != null)
            {
                Message("rollback");
            }
			
			docWriter.PauseAllThreads();
			
			try
			{
				FinishMerges(false, state);
				
				// Must pre-close these two, in case they increment
				// changeCount so that we can then set it to false
				// before calling closeInternal
				mergePolicy.Close();
				mergeScheduler.Close();
				
				lock (this)
				{
					
					if (pendingCommit != null)
					{
						pendingCommit.RollbackCommit(directory, state);
						deleter.DecRef(pendingCommit, state);
						pendingCommit = null;
						System.Threading.Monitor.PulseAll(this);
					}
					
					// Keep the same segmentInfos instance but replace all
					// of its SegmentInfo instances.  This is so the next
					// attempt to commit using this instance of IndexWriter
					// will always write to a new generation ("write
					// once").
					segmentInfos.Clear();
					segmentInfos.AddRange(rollbackSegmentInfos);
					
					System.Diagnostics.Debug.Assert(!HasExternalSegments());
					
					docWriter.Abort();
					
					System.Diagnostics.Debug.Assert(TestPoint("rollback before checkpoint"));
					
					// Ask deleter to locate unreferenced files & remove
					// them:
					deleter.Checkpoint(segmentInfos, false, state);
					deleter.Refresh(state);
				}
				
				// Don't bother saving any changes in our segmentInfos
				readerPool.Clear(null);
				
				lastCommitChangeCount = changeCount;
				
				success = true;
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "rollbackInternal");
			}
			finally
			{
				lock (this)
				{
					if (!success)
					{
						docWriter.ResumeAllThreads();
						closing = false;
						System.Threading.Monitor.PulseAll(this);
						if (infoStream != null)
							Message("hit exception during rollback");
					}
				}
			}
			
			CloseInternal(false, state);
		}
		
		/// <summary> Delete all documents in the index.
		/// 
		/// <p/>This method will drop all buffered documents and will 
		/// remove all segments from the index. This change will not be
		/// visible until a <see cref="Commit()" /> has been called. This method
		/// can be rolled back using <see cref="Rollback()" />.<p/>
		/// 
		/// <p/>NOTE: this method is much faster than using deleteDocuments( new MatchAllDocsQuery() ).<p/>
		/// 
		/// <p/>NOTE: this method will forcefully abort all merges
		/// in progress.  If other threads are running <see cref="Optimize()" />
		/// or any of the addIndexes methods, they
		/// will receive <see cref="Net.Index.MergePolicy.MergeAbortedException" />s.
		/// </summary>
		public virtual void  DeleteAll(IState state)
		{
			lock (this)
			{
				docWriter.PauseAllThreads();
				try
				{
					
					// Abort any running merges
					FinishMerges(false, state);
					
					// Remove any buffered docs
					docWriter.Abort();
					docWriter.SetFlushedDocCount(0);
					
					// Remove all segments
					segmentInfos.Clear();
					
					// Ask deleter to locate unreferenced files & remove them:
					deleter.Checkpoint(segmentInfos, false, state);
					deleter.Refresh(state);
					
					// Don't bother saving any changes in our segmentInfos
					readerPool.Clear(null);
					
					// Mark that the index has changed
					++changeCount;
				}
				catch (System.OutOfMemoryException oom)
				{
					HandleOOM(oom, "deleteAll");
				}
				finally
				{
					docWriter.ResumeAllThreads();
					if (infoStream != null)
					{
						Message("hit exception during deleteAll");
					}
				}
			}
		}
		
		private void  FinishMerges(bool waitForMerges, IState state)
		{
			lock (this)
			{
				if (!waitForMerges)
				{
					
					stopMerges = true;
					
					// Abort all pending & running merges:
					foreach(MergePolicy.OneMerge merge in pendingMerges)
					{
						if (infoStream != null)
							Message("now abort pending merge " + merge.SegString(directory, state));
						merge.Abort();
						MergeFinish(merge);
					}
					pendingMerges.Clear();
					
					foreach(MergePolicy.OneMerge merge in runningMerges)
					{
						if (infoStream != null)
							Message("now abort running merge " + merge.SegString(directory, state));
						merge.Abort();
					}
					
					// Ensure any running addIndexes finishes.  It's fine
					// if a new one attempts to start because its merges
					// will quickly see the stopMerges == true and abort.
					AcquireRead();
					ReleaseRead();
					
					// These merges periodically check whether they have
					// been aborted, and stop if so.  We wait here to make
					// sure they all stop.  It should not take very long
					// because the merge threads periodically check if
					// they are aborted.
					while (runningMerges.Count > 0)
					{
						if (infoStream != null)
							Message("now wait for " + runningMerges.Count + " running merge to abort");
						DoWait();
					}
					
					stopMerges = false;
					System.Threading.Monitor.PulseAll(this);
					
					System.Diagnostics.Debug.Assert(0 == mergingSegments.Count);
					
					if (infoStream != null)
						Message("all running merges have aborted");
				}
				else
				{
					// waitForMerges() will ensure any running addIndexes finishes.  
					// It's fine if a new one attempts to start because from our
					// caller above the call will see that we are in the
					// process of closing, and will throw an
					// AlreadyClosedException.
					WaitForMerges();
				}
			}
		}
		
		/// <summary> Wait for any currently outstanding merges to finish.
		/// 
		/// <p/>It is guaranteed that any merges started prior to calling this method 
		/// will have completed once this method completes.<p/>
		/// </summary>
		public virtual void  WaitForMerges()
		{
			lock (this)
			{
				// Ensure any running addIndexes finishes.
				AcquireRead();
				ReleaseRead();
				
				while (pendingMerges.Count > 0 || runningMerges.Count > 0)
				{
					DoWait();
				}
				
				// sanity check
				System.Diagnostics.Debug.Assert(0 == mergingSegments.Count);
			}
		}
		
		/*
		* Called whenever the SegmentInfos has been updated and
		* the index files referenced exist (correctly) in the
		* index directory.
		*/
		private void  Checkpoint(IState state)
		{
			lock (this)
			{
				changeCount++;
				deleter.Checkpoint(segmentInfos, false, state);
			}
		}
		
		private void  FinishAddIndexes()
		{
			ReleaseWrite();
		}
		
		private void  BlockAddIndexes(bool includePendingClose)
		{
			
			AcquireRead();
			
			bool success = false;
			try
			{
				
				// Make sure we are still open since we could have
				// waited quite a while for last addIndexes to finish
				EnsureOpen(includePendingClose);
				success = true;
			}
			finally
			{
				if (!success)
					ReleaseRead();
			}
		}
		
		private void  ResumeAddIndexes()
		{
			ReleaseRead();
		}
		
		private void  ResetMergeExceptions()
		{
			lock (this)
			{
				mergeExceptions = new List<MergePolicy.OneMerge>();
				mergeGen++;
			}
		}
		
		private void  NoDupDirs(Directory[] dirs)
		{
            HashSet<Directory> dups = new HashSet<Directory>();
			for (int i = 0; i < dirs.Length; i++)
			{
                if (dups.Contains(dirs[i]))
				{
					throw new System.ArgumentException("Directory " + dirs[i] + " appears more than once");
				}
				if (dirs[i] == directory)
					throw new System.ArgumentException("Cannot add directory to itself");
                dups.Add(dirs[i]);
            }
		}
		
		/// <summary> Merges all segments from an array of indexes into this
		/// index.
		/// 
		/// <p/>This may be used to parallelize batch indexing.  A large document
		/// collection can be broken into sub-collections.  Each sub-collection can be
		/// indexed in parallel, on a different thread, process or machine.  The
		/// complete index can then be created by merging sub-collection indexes
		/// with this method.
		/// 
		/// <p/><b>NOTE:</b> the index in each Directory must not be
		/// changed (opened by a writer) while this method is
		/// running.  This method does not acquire a write lock in
		/// each input Directory, so it is up to the caller to
		/// enforce this.
		/// 
		/// <p/><b>NOTE:</b> while this is running, any attempts to
		/// add or delete documents (with another thread) will be
		/// paused until this method completes.
		/// 
		/// <p/>This method is transactional in how Exceptions are
		/// handled: it does not commit a new segments_N file until
		/// all indexes are added.  This means if an Exception
		/// occurs (for example disk full), then either no indexes
		/// will have been added or they all will have been.<p/>
		/// 
		/// <p/>Note that this requires temporary free space in the
		/// Directory up to 2X the sum of all input indexes
		/// (including the starting index).  If readers/searchers
		/// are open against the starting index, then temporary
		/// free space required will be higher by the size of the
		/// starting index (see <see cref="Optimize()" /> for details).
		/// <p/>
		/// 
		/// <p/>Once this completes, the final size of the index
		/// will be less than the sum of all input index sizes
		/// (including the starting index).  It could be quite a
		/// bit smaller (if there were many pending deletes) or
		/// just slightly smaller.<p/>
		/// 
		/// <p/>
		/// This requires this index not be among those to be added.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  AddIndexesNoOptimize(IState state, params Directory[] dirs)
		{
			
			EnsureOpen();
			
			NoDupDirs(dirs);
			
			// Do not allow add docs or deletes while we are running:
			docWriter.PauseAllThreads();
			
			try
			{
				if (infoStream != null)
					Message("flush at addIndexesNoOptimize");
				Flush(true, false, true, state);
				
				bool success = false;
				
				StartTransaction(false, state);
				
				try
				{
					
					int docCount = 0;
					lock (this)
					{
						EnsureOpen();
						
						for (int i = 0; i < dirs.Length; i++)
						{
							if (directory == dirs[i])
							{
								// cannot add this index: segments may be deleted in merge before added
								throw new System.ArgumentException("Cannot add this index to itself");
							}
							
							SegmentInfos sis = new SegmentInfos(); // read infos from dir
							sis.Read(dirs[i], state);
							for (int j = 0; j < sis.Count; j++)
							{
								SegmentInfo info = sis.Info(j);
								System.Diagnostics.Debug.Assert(!segmentInfos.Contains(info), "dup info dir=" + info.dir + " name=" + info.name);
								docCount += info.docCount;
								segmentInfos.Add(info); // add each info
							}
						}
					}
					
					// Notify DocumentsWriter that the flushed count just increased
					docWriter.UpdateFlushedDocCount(docCount);
					
					MaybeMerge(state);
					
					EnsureOpen();
					
					// If after merging there remain segments in the index
					// that are in a different directory, just copy these
					// over into our index.  This is necessary (before
					// finishing the transaction) to avoid leaving the
					// index in an unusable (inconsistent) state.
					ResolveExternalSegments(state);
					
					EnsureOpen();
					
					success = true;
				}
				finally
				{
					if (success)
					{
						CommitTransaction(state);
					}
					else
					{
						RollbackTransaction(state);
					}
				}
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "addIndexesNoOptimize");
			}
			finally
			{
				if (docWriter != null)
				{
					docWriter.ResumeAllThreads();
				}
			}
		}
		
		private bool HasExternalSegments()
		{
			return segmentInfos.HasExternalSegments(directory);
		}
		
		/* If any of our segments are using a directory != ours
		* then we have to either copy them over one by one, merge
		* them (if merge policy has chosen to) or wait until
		* currently running merges (in the background) complete.
		* We don't return until the SegmentInfos has no more
		* external segments.  Currently this is only used by
		* addIndexesNoOptimize(). */
		private void  ResolveExternalSegments(IState state)
		{
			
			bool any = false;
			
			bool done = false;
			
			while (!done)
			{
				SegmentInfo info = null;
				MergePolicy.OneMerge merge = null;
				lock (this)
				{
					
					if (stopMerges)
						throw new MergePolicy.MergeAbortedException("rollback() was called or addIndexes* hit an unhandled exception");
					
					int numSegments = segmentInfos.Count;
					
					done = true;
					for (int i = 0; i < numSegments; i++)
					{
						info = segmentInfos.Info(i);
						if (info.dir != directory)
						{
							done = false;
							MergePolicy.OneMerge newMerge = new MergePolicy.OneMerge(segmentInfos.Range(i, 1 + i), mergePolicy is LogMergePolicy && UseCompoundFile);
							
							// Returns true if no running merge conflicts
							// with this one (and, records this merge as
							// pending), ie, this segment is not currently
							// being merged:
							if (RegisterMerge(newMerge, state))
							{
								merge = newMerge;
								
								// If this segment is not currently being
								// merged, then advance it to running & run
								// the merge ourself (below):
                                pendingMerges.Remove(merge);    // {{Aroush-2.9}} From Mike Garski: this is an O(n) op... is that an issue?
								runningMerges.Add(merge);
								break;
							}
						}
					}
					
					if (!done && merge == null)
					// We are not yet done (external segments still
					// exist in segmentInfos), yet, all such segments
					// are currently "covered" by a pending or running
					// merge.  We now try to grab any pending merge
					// that involves external segments:
						merge = GetNextExternalMerge();
					
					if (!done && merge == null)
					// We are not yet done, and, all external segments
					// fall under merges that the merge scheduler is
					// currently running.  So, we now wait and check
					// back to see if the merge has completed.
						DoWait();
				}
				
				if (merge != null)
				{
					any = true;
					Merge(merge, state);
				}
			}
			
			if (any)
			// Sometimes, on copying an external segment over,
			// more merges may become necessary:
				mergeScheduler.Merge(this, state);
		}
		
		/// <summary>Merges the provided indexes into this index.
		/// <p/>After this completes, the index is optimized. <p/>
		/// <p/>The provided IndexReaders are not closed.<p/>
		/// 
		/// <p/><b>NOTE:</b> while this is running, any attempts to
		/// add or delete documents (with another thread) will be
		/// paused until this method completes.
		/// 
		/// <p/>See <see cref="AddIndexesNoOptimize(Directory[])" /> for
		/// details on transactional semantics, temporary free
		/// space required in the Directory, and non-CFS segments
		/// on an Exception.<p/>
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  AddIndexes(IState state, params IndexReader[] readers)
		{
			
			EnsureOpen();
			
			// Do not allow add docs or deletes while we are running:
			docWriter.PauseAllThreads();
			
			// We must pre-acquire a read lock here (and upgrade to
			// write lock in startTransaction below) so that no
			// other addIndexes is allowed to start up after we have
			// flushed & optimized but before we then start our
			// transaction.  This is because the merging below
			// requires that only one segment is present in the
			// index:
			AcquireRead();
			
			try
			{
				
				SegmentInfo info = null;
				System.String mergedName = null;
				SegmentMerger merger = null;
				
				bool success = false;
				
				try
				{
					Flush(true, false, true, state);
					Optimize(state, CancellationToken.None); // start with zero or 1 seg
					success = true;
				}
				finally
				{
					// Take care to release the read lock if we hit an
					// exception before starting the transaction
					if (!success)
						ReleaseRead();
				}
				
				// true means we already have a read lock; if this
				// call hits an exception it will release the write
				// lock:
				StartTransaction(true, state);
				
				try
				{
					mergedName = NewSegmentName();
					merger = new SegmentMerger(this, mergedName, null);
					
					SegmentReader sReader = null;
					lock (this)
					{
						if (segmentInfos.Count == 1)
						{
							// add existing index, if any
							sReader = readerPool.Get(segmentInfos.Info(0), true, BufferedIndexInput.BUFFER_SIZE, - 1, state);
						}
					}
					
					success = false;
					
					try
					{
						if (sReader != null)
							merger.Add(sReader);
						
						for (int i = 0; i < readers.Length; i++)
						// add new indexes
							merger.Add(readers[i]);
						
						int docCount = merger.Merge(state); // merge 'em
						
						lock (this)
						{
							segmentInfos.Clear(); // pop old infos & add new
							info = new SegmentInfo(mergedName, docCount, directory, false, true, - 1, null, false, merger.HasProx());
							SetDiagnostics(info, "addIndexes(params IndexReader[])");
							segmentInfos.Add(info);
						}
						
						// Notify DocumentsWriter that the flushed count just increased
						docWriter.UpdateFlushedDocCount(docCount);
						
						success = true;
					}
					finally
					{
						if (sReader != null)
						{
							readerPool.Release(sReader, state);
						}
					}
				}
				finally
				{
					if (!success)
					{
						if (infoStream != null)
							Message("hit exception in addIndexes during merge");
						RollbackTransaction(state);
					}
					else
					{
						CommitTransaction(state);
					}
				}
				
				if (mergePolicy is LogMergePolicy && UseCompoundFile)
				{
					
					IList<string> files = null;
					
					lock (this)
					{
						// Must incRef our files so that if another thread
						// is running merge/optimize, it doesn't delete our
						// segment's files before we have a change to
						// finish making the compound file.
						if (segmentInfos.Contains(info))
						{
							files = info.Files(state);
							deleter.IncRef(files);
						}
					}
					
					if (files != null)
					{
						
						success = false;
						
						StartTransaction(false, state);
						
						try
						{
							merger.CreateCompoundFile(mergedName + ".cfs");
							lock (this)
							{
								info.SetUseCompoundFile(true);
							}
							
							success = true;
						}
						finally
						{
                            lock (this)
                            {
                                deleter.DecRef(files, state);
                            }
														
							if (!success)
							{
								if (infoStream != null)
									Message("hit exception building compound file in addIndexes during merge");
								
								RollbackTransaction(state);
							}
							else
							{
								CommitTransaction(state);
							}
						}
					}
				}
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "addIndexes(params IndexReader[])");
			}
			finally
			{
				if (docWriter != null)
				{
					docWriter.ResumeAllThreads();
				}
			}
		}

        ///<summary>
        /// A hook for extending classes to execute operations after pending added and
        /// deleted documents have been flushed to the Directory but before the change
        /// is committed (new segments_N file written).
        ///</summary>   
		protected  virtual void  DoAfterFlush()
		{
		}

        ///<summary>
        /// A hook for extending classes to execute operations before pending added and
        /// deleted documents are flushed to the Directory.
        ///</summary>
        protected virtual void DoBeforeFlush() 
        {
        }
		
		/// <summary>Expert: prepare for commit.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
        /// <seealso cref="PrepareCommit(IDictionary{string,string})">
		/// </seealso>
		public void  PrepareCommit(IState state)
		{
			EnsureOpen();
			PrepareCommit(null, state);
		}
		
		/// <summary><p/>Expert: prepare for commit, specifying
		/// commitUserData Map (String -> String).  This does the
		/// first phase of 2-phase commit. This method does all steps
		/// necessary to commit changes since this writer was
		/// opened: flushes pending added and deleted docs, syncs
		/// the index files, writes most of next segments_N file.
		/// After calling this you must call either <see cref="Commit()" />
		/// to finish the commit, or <see cref="Rollback()" />
		/// to revert the commit and undo all changes
		/// done since the writer was opened.<p/>
		/// 
        /// You can also just call <see cref="Commit(IDictionary{string,string})" /> directly
		/// without prepareCommit first in which case that method
		/// will internally call prepareCommit.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <param name="commitUserData">Opaque Map (String->String)
		/// that's recorded into the segments file in the index,
		/// and retrievable by <see cref="IndexReader.GetCommitUserData" />.
		/// Note that when IndexWriter commits itself, during <see cref="Close()" />, the
		/// commitUserData is unchanged (just carried over from
		/// the prior commit).  If this is null then the previous
		/// commitUserData is kept.  Also, the commitUserData will
		/// only "stick" if there are actually changes in the
		/// index to commit.
		/// </param>
        private void PrepareCommit(IDictionary<string, string> commitUserData, IState state)
		{
			if (hitOOM)
			{
				throw new System.SystemException("this writer hit an OutOfMemoryError; cannot commit");
			}
			
			if (pendingCommit != null)
				throw new System.SystemException("prepareCommit was already called with no corresponding call to commit");
			
			if (infoStream != null)
				Message("prepareCommit: flush");
			
			Flush(true, true, true, state);
			
			StartCommit(0, commitUserData, state);
		}
		
        // Used only by commit, below; lock order is commitLock -> IW
        private Object commitLock = new Object();

		private void  Commit(long sizeInBytes, IState state)
		{
            lock(commitLock) {
                StartCommit(sizeInBytes, null, state);
                FinishCommit(state);
            }
		}
		
		/// <summary> <p/>Commits all pending changes (added &amp; deleted
		/// documents, optimizations, segment merges, added
		/// indexes, etc.) to the index, and syncs all referenced
		/// index files, such that a reader will see the changes
		/// and the index updates will survive an OS or machine
		/// crash or power loss.  Note that this does not wait for
		/// any running background merges to finish.  This may be a
		/// costly operation, so you should test the cost in your
		/// application and do it only when really necessary.<p/>
		/// 
		/// <p/> Note that this operation calls Directory.sync on
		/// the index files.  That call should not return until the
		/// file contents &amp; metadata are on stable storage.  For
		/// FSDirectory, this calls the OS's fsync.  But, beware:
		/// some hardware devices may in fact cache writes even
		/// during fsync, and return before the bits are actually
		/// on stable storage, to give the appearance of faster
		/// performance.  If you have such a device, and it does
		/// not have a battery backup (for example) then on power
		/// loss it may still lose data.  Lucene cannot guarantee
		/// consistency on such devices.  <p/>
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// 
		/// </summary>
		/// <seealso cref="PrepareCommit()">
		/// </seealso>
        /// <seealso cref="Commit(IDictionary{string,string})">
		/// </seealso>
		public void  Commit(IState state)
		{
			Commit(null, state);
		}
		
		/// <summary>Commits all changes to the index, specifying a
		/// commitUserData Map (String -> String).  This just
		/// calls <see cref="PrepareCommit(IDictionary{string, string})" /> (if you didn't
		/// already call it) and then <see cref="FinishCommit" />.
		/// 
		/// <p/><b>NOTE</b>: if this method hits an OutOfMemoryError
		/// you should immediately close the writer.  See <a
		/// href="#OOME">above</a> for details.<p/>
		/// </summary>
        public void Commit(IDictionary<string, string> commitUserData, IState state)
		{
			EnsureOpen();

            if (infoStream != null)
            {
                Message("commit: start");
            }

            lock (commitLock)
            {
                if (infoStream != null)
                {
                    Message("commit: enter lock");
                }
                if (pendingCommit == null)
                {
                    if (infoStream != null)
                    {
                        Message("commit: now prepare");
                    }
                    PrepareCommit(commitUserData, state);
                }
                else if (infoStream != null)
                {
                    Message("commit: already prepared");
                }

                FinishCommit(state);
            }
		}
		
		private void  FinishCommit(IState state)
		{
			lock (this)
			{
				
				if (pendingCommit != null)
				{
					try
					{
						if (infoStream != null)
							Message("commit: pendingCommit != null");
						pendingCommit.FinishCommit(directory, state);
						if (infoStream != null)
							Message("commit: wrote segments file \"" + pendingCommit.GetCurrentSegmentFileName() + "\"");
						lastCommitChangeCount = pendingCommitChangeCount;
						segmentInfos.UpdateGeneration(pendingCommit);
						segmentInfos.UserData = pendingCommit.UserData;
						SetRollbackSegmentInfos(pendingCommit);
						deleter.Checkpoint(pendingCommit, true, state);
					}
					finally
					{
						deleter.DecRef(pendingCommit, state);
						pendingCommit = null;
						System.Threading.Monitor.PulseAll(this);
					}
				}
                else if (infoStream != null)
                {
                    Message("commit: pendingCommit == null; skip");
                }

                if (infoStream != null)
                {
                    Message("commit: done");
                }
			}
		}
		
		/// <summary> Flush all in-memory buffered udpates (adds and deletes)
		/// to the Directory.
		/// </summary>
		/// <param name="triggerMerge">if true, we may merge segments (if
		/// deletes or docs were flushed) if necessary
		/// </param>
		/// <param name="flushDocStores">if false we are allowed to keep
		/// doc stores open to share with the next segment
		/// </param>
		/// <param name="flushDeletes">whether pending deletes should also
		/// be flushed
		/// </param>
		public /*protected internal*/ void  Flush(bool triggerMerge, bool flushDocStores, bool flushDeletes, IState state)
		{
			// We can be called during close, when closing==true, so we must pass false to ensureOpen:
			EnsureOpen(false);
			if (DoFlush(flushDocStores, flushDeletes, state) && triggerMerge)
				MaybeMerge(state);
		}
		
		// TODO: this method should not have to be entirely
		// synchronized, ie, merges should be allowed to commit
		// even while a flush is happening
		private bool DoFlush(bool flushDocStores, bool flushDeletes, IState state)
		{
			lock (this)
			{
                try
                {
                    try
                    {
                        return DoFlushInternal(flushDocStores, flushDeletes, state);
                    }
                    finally
                    {
                        if (docWriter.DoBalanceRAM())
                        {
                            docWriter.BalanceRAM();
                        }
                    }
                }
                finally
                {
                    docWriter.ClearFlushPending();
                }
			}
		}
		
		// TODO: this method should not have to be entirely
		// synchronized, ie, merges should be allowed to commit
		// even while a flush is happening
		private bool DoFlushInternal(bool flushDocStores, bool flushDeletes, IState state)
		{
			lock (this)
			{
				if (hitOOM)
				{
					throw new System.SystemException("this writer hit an OutOfMemoryError; cannot flush");
				}
				
				EnsureOpen(false);
				
				System.Diagnostics.Debug.Assert(TestPoint("startDoFlush"));

                DoBeforeFlush();
				
				flushCount++;
				
				// If we are flushing because too many deletes
				// accumulated, then we should apply the deletes to free
				// RAM:
				flushDeletes |= docWriter.DoApplyDeletes();
				
				// Make sure no threads are actively adding a document.
				// Returns true if docWriter is currently aborting, in
				// which case we skip flushing this segment
                if (infoStream != null)
                {
                    Message("flush: now pause all indexing threads");
                }
				if (docWriter.PauseAllThreads())
				{
					docWriter.ResumeAllThreads();
					return false;
				}
				
				try
				{
					
					SegmentInfo newSegment = null;
					
					int numDocs = docWriter.NumDocsInRAM;
					
					// Always flush docs if there are any
					bool flushDocs = numDocs > 0;
					
					System.String docStoreSegment = docWriter.DocStoreSegment;

                    System.Diagnostics.Debug.Assert(docStoreSegment != null || numDocs == 0, "dss=" + docStoreSegment + " numDocs=" + numDocs);
					
					if (docStoreSegment == null)
						flushDocStores = false;
					
					int docStoreOffset = docWriter.DocStoreOffset;
					
					bool docStoreIsCompoundFile = false;
					
					if (infoStream != null)
					{
						Message("  flush: segment=" + docWriter.Segment + " docStoreSegment=" + docWriter.DocStoreSegment + " docStoreOffset=" + docStoreOffset + " flushDocs=" + flushDocs + " flushDeletes=" + flushDeletes + " flushDocStores=" + flushDocStores + " numDocs=" + numDocs + " numBufDelTerms=" + docWriter.GetNumBufferedDeleteTerms());
						Message("  index before flush " + SegString(state));
					}
					
					// Check if the doc stores must be separately flushed
					// because other segments, besides the one we are about
					// to flush, reference it
					if (flushDocStores && (!flushDocs || !docWriter.Segment.Equals(docWriter.DocStoreSegment)))
					{
						// We must separately flush the doc store
						if (infoStream != null)
							Message("  flush shared docStore segment " + docStoreSegment);
						
						docStoreIsCompoundFile = FlushDocStores(state);
						flushDocStores = false;
					}
					
					System.String segment = docWriter.Segment;
					
					// If we are flushing docs, segment must not be null:
					System.Diagnostics.Debug.Assert(segment != null || !flushDocs);
					
					if (flushDocs)
					{
						
						bool success = false;
						int flushedDocCount;
						
						try
						{
							flushedDocCount = docWriter.Flush(flushDocStores, state);
                            if (infoStream != null)
                            {
                                Message("flushedFiles=" + docWriter.GetFlushedFiles());
                            }
							success = true;
						}
						finally
						{
							if (!success)
							{
								if (infoStream != null)
									Message("hit exception flushing segment " + segment);
								deleter.Refresh(segment, state);
							}
						}
						
						if (0 == docStoreOffset && flushDocStores)
						{
							// This means we are flushing private doc stores
							// with this segment, so it will not be shared
							// with other segments
							System.Diagnostics.Debug.Assert(docStoreSegment != null);
							System.Diagnostics.Debug.Assert(docStoreSegment.Equals(segment));
							docStoreOffset = - 1;
							docStoreIsCompoundFile = false;
							docStoreSegment = null;
						}
						
						// Create new SegmentInfo, but do not add to our
						// segmentInfos until deletes are flushed
						// successfully.
						newSegment = new SegmentInfo(segment, flushedDocCount, directory, false, true, docStoreOffset, docStoreSegment, docStoreIsCompoundFile, docWriter.HasProx());
						SetDiagnostics(newSegment, "flush");
					}
					
					docWriter.PushDeletes();
					
					if (flushDocs)
					{
						segmentInfos.Add(newSegment);
						Checkpoint(state);
					}
					
					if (flushDocs && mergePolicy.UseCompoundFile(segmentInfos, newSegment))
					{
						// Now build compound file
						bool success = false;
						try
						{
							docWriter.CreateCompoundFile(segment);
							success = true;
						}
						finally
						{
							if (!success)
							{
								if (infoStream != null)
									Message("hit exception creating compound file for newly flushed segment " + segment);
								deleter.DeleteFile(segment + "." + IndexFileNames.COMPOUND_FILE_EXTENSION, state);
							}
						}
						
						newSegment.SetUseCompoundFile(true);
						Checkpoint(state);
					}
					
					if (flushDeletes)
					{
						ApplyDeletes(state);
					}
					
					if (flushDocs)
						Checkpoint(state);
					
					DoAfterFlush();
					
					return flushDocs;
				}
				catch (System.OutOfMemoryException oom)
				{
					HandleOOM(oom, "doFlush");
					// never hit
					return false;
				}
				finally
				{
					docWriter.ResumeAllThreads();
				}
			}
		}
		
		/// <summary>Expert:  Return the total size of all index files currently cached in memory.
		/// Useful for size management with flushRamDocs()
		/// </summary>
		public long RamSizeInBytes()
		{
			EnsureOpen();
			return docWriter.GetRAMUsed();
		}
		
		/// <summary>Expert:  Return the number of documents currently
		/// buffered in RAM. 
		/// </summary>
		public int NumRamDocs()
		{
			lock (this)
			{
				EnsureOpen();
				return docWriter.NumDocsInRAM;
			}
		}
		
		private int EnsureContiguousMerge(MergePolicy.OneMerge merge, IState state)
		{
			
			int first = segmentInfos.IndexOf(merge.segments.Info(0));
			if (first == - 1)
				throw new MergePolicy.MergeException("could not find segment " + merge.segments.Info(0).name + " in current index " + SegString(state), directory);
			
			int numSegments = segmentInfos.Count;
			
			int numSegmentsToMerge = merge.segments.Count;
			for (int i = 0; i < numSegmentsToMerge; i++)
			{
				SegmentInfo info = merge.segments.Info(i);
				
				if (first + i >= numSegments || !segmentInfos.Info(first + i).Equals(info))
				{
					if (segmentInfos.IndexOf(info) == - 1)
						throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.name + ") that is not in the current index " + SegString(state), directory);
					else
						throw new MergePolicy.MergeException("MergePolicy selected non-contiguous segments to merge (" + merge.SegString(directory, state) + " vs " + SegString(state) + "), which IndexWriter (currently) cannot handle", directory);
				}
			}
			
			return first;
		}
		
		/// <summary>Carefully merges deletes for the segments we just
		/// merged.  This is tricky because, although merging will
		/// clear all deletes (compacts the documents), new
		/// deletes may have been flushed to the segments since
		/// the merge was started.  This method "carries over"
		/// such new deletes onto the newly merged segment, and
		/// saves the resulting deletes file (incrementing the
		/// delete generation for merge.info).  If no deletes were
		/// flushed, no new deletes file is saved. 
		/// </summary>
		private void  CommitMergedDeletes(MergePolicy.OneMerge merge, SegmentReader mergeReader, IState state)
		{
			lock (this)
			{
				
				System.Diagnostics.Debug.Assert(TestPoint("startCommitMergeDeletes"));
				
				SegmentInfos sourceSegments = merge.segments;
				
				if (infoStream != null)
					Message("commitMergeDeletes " + merge.SegString(directory, state));
				
				// Carefully merge deletes that occurred after we
				// started merging:
				int docUpto = 0;
				int delCount = 0;
				
				for (int i = 0; i < sourceSegments.Count; i++)
				{
					SegmentInfo info = sourceSegments.Info(i);
					int docCount = info.docCount;
					SegmentReader previousReader = merge.readersClone[i];
					SegmentReader currentReader = merge.readers[i];
					if (previousReader.HasDeletions)
					{
						
						// There were deletes on this segment when the merge
						// started.  The merge has collapsed away those
						// deletes, but, if new deletes were flushed since
						// the merge started, we must now carefully keep any
						// newly flushed deletes but mapping them to the new
						// docIDs.
						
						if (currentReader.NumDeletedDocs > previousReader.NumDeletedDocs)
						{
							// This means this segment has had new deletes
							// committed since we started the merge, so we
							// must merge them:
							for (int j = 0; j < docCount; j++)
							{
								if (previousReader.IsDeleted(j))
								{
									System.Diagnostics.Debug.Assert(currentReader.IsDeleted(j));
                                }
								else
								{
									if (currentReader.IsDeleted(j))
									{
										mergeReader.DoDelete(docUpto, state);
										delCount++;
									}
									docUpto++;
								}
							}
						}
						else
						{
							docUpto += docCount - previousReader.NumDeletedDocs;
						}
					}
					else if (currentReader.HasDeletions)
					{
						// This segment had no deletes before but now it
						// does:
						for (int j = 0; j < docCount; j++)
						{
							if (currentReader.IsDeleted(j))
							{
								mergeReader.DoDelete(docUpto, state);
								delCount++;
							}
							docUpto++;
						}
					}
					// No deletes before or after
					else
						docUpto += info.docCount;
				}
				
				System.Diagnostics.Debug.Assert(mergeReader.NumDeletedDocs == delCount);
				
				mergeReader.hasChanges = delCount > 0;
			}
		}
		
		/* FIXME if we want to support non-contiguous segment merges */
		private bool CommitMerge(MergePolicy.OneMerge merge, SegmentMerger merger, int mergedDocCount, SegmentReader mergedReader, IState state)
		{
			lock (this)
			{
				
				System.Diagnostics.Debug.Assert(TestPoint("startCommitMerge"));
				
				if (hitOOM)
				{
					throw new System.SystemException("this writer hit an OutOfMemoryError; cannot complete merge");
				}
				
				if (infoStream != null)
					Message("commitMerge: " + merge.SegString(directory, state) + " index=" + SegString(state));
				
				System.Diagnostics.Debug.Assert(merge.registerDone);
				
				// If merge was explicitly aborted, or, if rollback() or
				// rollbackTransaction() had been called since our merge
				// started (which results in an unqualified
				// deleter.refresh() call that will remove any index
				// file that current segments does not reference), we
				// abort this merge
				if (merge.IsAborted())
				{
					if (infoStream != null)
						Message("commitMerge: skipping merge " + merge.SegString(directory, state) + ": it was aborted");
					
					return false;
				}
				
				int start = EnsureContiguousMerge(merge, state);
				
				CommitMergedDeletes(merge, mergedReader, state);
				docWriter.RemapDeletes(segmentInfos, merger.GetDocMaps(), merger.GetDelCounts(), merge, mergedDocCount);

                // If the doc store we are using has been closed and
                // is in now compound format (but wasn't when we
                // started), then we will switch to the compound
                // format as well:
                SetMergeDocStoreIsCompoundFile(merge);
				
				merge.info.HasProx = merger.HasProx();
				
				segmentInfos.RemoveRange(start, start + merge.segments.Count - start);
				System.Diagnostics.Debug.Assert(!segmentInfos.Contains(merge.info));
				segmentInfos.Insert(start, merge.info);

                CloseMergeReaders(merge, false, state);
				
				// Must note the change to segmentInfos so any commits
				// in-flight don't lose it:
				Checkpoint(state);
				
				// If the merged segments had pending changes, clear
				// them so that they don't bother writing them to
				// disk, updating SegmentInfo, etc.:
				readerPool.Clear(merge.segments);

                if (merge.optimize)
                {
                    // cascade the optimize:
                    segmentsToOptimize.Add(merge.info);
                }
				return true;
			}
		}
		
		private void HandleMergeException(System.Exception t, MergePolicy.OneMerge merge, IState state)
		{
			if (infoStream != null)
			{
				Message("handleMergeException: merge=" + merge.SegString(directory, state) + " exc=" + t);
			}
			
			// Set the exception on the merge, so if
			// optimize() is waiting on us it sees the root
			// cause exception:
			merge.SetException(t);
			AddMergeException(merge);
			
			if (t is MergePolicy.MergeAbortedException)
			{
				// We can ignore this exception (it happens when
				// close(false) or rollback is called), unless the
				// merge involves segments from external directories,
				// in which case we must throw it so, for example, the
				// rollbackTransaction code in addIndexes* is
				// executed.
                if (merge.isExternal)
                {
                    var exceptionDispatchInfo = ExceptionDispatchInfo.Capture(t);
                    exceptionDispatchInfo.Throw();
                }
			}
            else if (t is System.IO.IOException || t is System.SystemException || t is System.ApplicationException)
            {
                var exceptionDispatchInfo = ExceptionDispatchInfo.Capture(t);
                exceptionDispatchInfo.Throw();
            }
            else
            {
                // Should not get here
                System.Diagnostics.Debug.Fail("Exception is not expected type!");
                throw new System.SystemException(null, t);
            }
		}
		
		public void Merge_ForNUnit(MergePolicy.OneMerge merge, IState state)
        {
            Merge(merge, state);
        }
		/// <summary> Merges the indicated segments, replacing them in the stack with a
		/// single segment.
		/// </summary>
		public void Merge(MergePolicy.OneMerge merge, IState state)
		{
			bool success = false;
			
			try
			{
				try
				{
					try
					{
						MergeInit(merge, state);
						
						if (infoStream != null)
						{
							Message("now merge\n  merge=" + merge.SegString(directory, state) + "\n  merge=" + merge + "\n  index=" + SegString(state));
						}
						
						MergeMiddle(merge, state);
						MergeSuccess(merge);
						success = true;
					}
					catch (System.Exception t)
					{
						HandleMergeException(t, merge, state);
					}
				}
				finally
				{
					lock (this)
					{
						MergeFinish(merge);
						
						if (!success)
						{
							if (infoStream != null)
								Message("hit exception during merge");
							if (merge.info != null && !segmentInfos.Contains(merge.info))
								deleter.Refresh(merge.info.name, state);
						}
						
						// This merge (and, generally, any change to the
						// segments) may now enable new merges, so we call
						// merge policy & update pending merges.
						if (success && !merge.IsAborted() && !closed && !closing)
							UpdatePendingMerges(merge.maxNumSegmentsOptimize, merge.optimize, state);
					}
				}
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "merge");
			}
		}
		
		/// <summary>Hook that's called when the specified merge is complete. </summary>
		internal virtual void  MergeSuccess(MergePolicy.OneMerge merge)
		{
		}
		
		/// <summary>Checks whether this merge involves any segments
		/// already participating in a merge.  If not, this merge
		/// is "registered", meaning we record that its segments
		/// are now participating in a merge, and true is
		/// returned.  Else (the merge conflicts) false is
		/// returned. 
		/// </summary>
		internal bool RegisterMerge(MergePolicy.OneMerge merge, IState state)
		{
			lock (this)
			{
				
				if (merge.registerDone)
					return true;
				
				if (stopMerges)
				{
					merge.Abort();
					throw new MergePolicy.MergeAbortedException("merge is aborted: " + merge.SegString(directory, state));
				}
				
				int count = merge.segments.Count;
				bool isExternal = false;
				for (int i = 0; i < count; i++)
				{
					SegmentInfo info = merge.segments.Info(i);
                    if (mergingSegments.Contains(info))
                    {
                        return false;
                    }
                    if (segmentInfos.IndexOf(info) == -1)
                    {
                        return false;
                    }
                    if (info.dir != directory)
                    {
                        isExternal = true;
                    }
                    if (segmentsToOptimize.Contains(info))
                    {
                        merge.optimize = true;
                        merge.maxNumSegmentsOptimize = optimizeMaxNumSegments;
                    }
				}
				
				EnsureContiguousMerge(merge, state);
				
				pendingMerges.AddLast(merge);
				
				if (infoStream != null)
					Message("add merge to pendingMerges: " + merge.SegString(directory, state) + " [total " + pendingMerges.Count + " pending]");
				
				merge.mergeGen = mergeGen;
				merge.isExternal = isExternal;
				
				// OK it does not conflict; now record that this merge
				// is running (while synchronized) to avoid race
				// condition where two conflicting merges from different
				// threads, start
                for (int i = 0; i < count; i++)
                {
                    SegmentInfo si = merge.segments.Info(i);
                    mergingSegments.Add(si);
                }
				
				// Merge is now registered
				merge.registerDone = true;
				return true;
			}
		}
		
		/// <summary>Does initial setup for a merge, which is fast but holds
		/// the synchronized lock on IndexWriter instance.  
		/// </summary>
		internal void  MergeInit(MergePolicy.OneMerge merge, IState state)
		{
			lock (this)
			{
				bool success = false;
				try
				{
					_MergeInit(merge, state);
					success = true;
				}
				finally
				{
					if (!success)
					{
						MergeFinish(merge);
					}
				}
			}
		}
		
		private void  _MergeInit(MergePolicy.OneMerge merge, IState state)
		{
			lock (this)
			{
				
				System.Diagnostics.Debug.Assert(TestPoint("startMergeInit"));
				
				System.Diagnostics.Debug.Assert(merge.registerDone);
				System.Diagnostics.Debug.Assert(!merge.optimize || merge.maxNumSegmentsOptimize > 0);
				
				if (hitOOM)
				{
					throw new System.SystemException("this writer hit an OutOfMemoryError; cannot merge");
				}
				
				if (merge.info != null)
				// mergeInit already done
					return ;
				
				if (merge.IsAborted())
					return ;
				
				ApplyDeletes(state);
				
				SegmentInfos sourceSegments = merge.segments;
				int end = sourceSegments.Count;
				
				// Check whether this merge will allow us to skip
				// merging the doc stores (stored field & vectors).
				// This is a very substantial optimization (saves tons
				// of IO).
				
				Directory lastDir = directory;
				System.String lastDocStoreSegment = null;
				int next = - 1;
				
				bool mergeDocStores = false;
				bool doFlushDocStore = false;
				System.String currentDocStoreSegment = docWriter.DocStoreSegment;
				
				// Test each segment to be merged: check if we need to
				// flush/merge doc stores
				for (int i = 0; i < end; i++)
				{
					SegmentInfo si = sourceSegments.Info(i);
					
					// If it has deletions we must merge the doc stores
					if (si.HasDeletions(state))
						mergeDocStores = true;
					
					// If it has its own (private) doc stores we must
					// merge the doc stores
					if (- 1 == si.DocStoreOffset)
						mergeDocStores = true;
					
					// If it has a different doc store segment than
					// previous segments, we must merge the doc stores
					System.String docStoreSegment = si.DocStoreSegment;
					if (docStoreSegment == null)
						mergeDocStores = true;
					else if (lastDocStoreSegment == null)
						lastDocStoreSegment = docStoreSegment;
					else if (!lastDocStoreSegment.Equals(docStoreSegment))
						mergeDocStores = true;
					
					// Segments' docScoreOffsets must be in-order,
					// contiguous.  For the default merge policy now
					// this will always be the case but for an arbitrary
					// merge policy this may not be the case
					if (- 1 == next)
						next = si.DocStoreOffset + si.docCount;
					else if (next != si.DocStoreOffset)
						mergeDocStores = true;
					else
						next = si.DocStoreOffset + si.docCount;
					
					// If the segment comes from a different directory
					// we must merge
					if (lastDir != si.dir)
						mergeDocStores = true;
					
					// If the segment is referencing the current "live"
					// doc store outputs then we must merge
					if (si.DocStoreOffset != - 1 && currentDocStoreSegment != null && si.DocStoreSegment.Equals(currentDocStoreSegment))
					{
						doFlushDocStore = true;
					}
				}

                // if a mergedSegmentWarmer is installed, we must merge
                // the doc stores because we will open a full
                // SegmentReader on the merged segment:
                if (!mergeDocStores && mergedSegmentWarmer != null && currentDocStoreSegment != null && lastDocStoreSegment != null && lastDocStoreSegment.Equals(currentDocStoreSegment))
                {
                    mergeDocStores = true;
                }

				int docStoreOffset;
				System.String docStoreSegment2;
				bool docStoreIsCompoundFile;
				
				if (mergeDocStores)
				{
					docStoreOffset = - 1;
					docStoreSegment2 = null;
					docStoreIsCompoundFile = false;
				}
				else
				{
					SegmentInfo si = sourceSegments.Info(0);
					docStoreOffset = si.DocStoreOffset;
					docStoreSegment2 = si.DocStoreSegment;
					docStoreIsCompoundFile = si.DocStoreIsCompoundFile;
				}
				
				if (mergeDocStores && doFlushDocStore)
				{
					// SegmentMerger intends to merge the doc stores
					// (stored fields, vectors), and at least one of the
					// segments to be merged refers to the currently
					// live doc stores.
					
					// TODO: if we know we are about to merge away these
					// newly flushed doc store files then we should not
					// make compound file out of them...
					if (infoStream != null)
						Message("now flush at merge");
					DoFlush(true, false, state);
				}
				
				merge.mergeDocStores = mergeDocStores;
				
				// Bind a new segment name here so even with
				// ConcurrentMergePolicy we keep deterministic segment
				// names.
				merge.info = new SegmentInfo(NewSegmentName(), 0, directory, false, true, docStoreOffset, docStoreSegment2, docStoreIsCompoundFile, false);


                IDictionary<string, string> details = new Dictionary<string, string>();
				details["optimize"] = merge.optimize + "";
				details["mergeFactor"] = end + "";
				details["mergeDocStores"] = mergeDocStores + "";
				SetDiagnostics(merge.info, "merge", details);
				
				// Also enroll the merged segment into mergingSegments;
				// this prevents it from getting selected for a merge
				// after our merge is done but while we are building the
				// CFS:
                mergingSegments.Add(merge.info);
			}
		}
		
		private void  SetDiagnostics(SegmentInfo info, System.String source)
		{
			SetDiagnostics(info, source, null);
		}

        private void SetDiagnostics(SegmentInfo info, System.String source, IDictionary<string, string> details)
		{
            IDictionary<string, string> diagnostics = new Dictionary<string,string>();
			diagnostics["source"] = source;
			diagnostics["lucene.version"] = Constants.LUCENE_VERSION;
			diagnostics["os"] = Constants.OS_NAME + "";
			diagnostics["os.arch"] = Constants.OS_ARCH + "";
			diagnostics["os.version"] = Constants.OS_VERSION + "";
			diagnostics["java.version"] = Constants.JAVA_VERSION + "";
			diagnostics["java.vendor"] = Constants.JAVA_VENDOR + "";
			if (details != null)
			{
				//System.Collections.ArrayList keys = new System.Collections.ArrayList(details.Keys);
				//System.Collections.ArrayList values = new System.Collections.ArrayList(details.Values);
                foreach (string key in details.Keys)
                {
                    diagnostics[key] = details[key];
                }
			}
			info.Diagnostics = diagnostics;
		}

		/// <summary>Does fininishing for a merge, which is fast but holds
		/// the synchronized lock on IndexWriter instance. 
		/// </summary>
		internal void  MergeFinish(MergePolicy.OneMerge merge)
		{
			lock (this)
			{
				
				// Optimize, addIndexes or finishMerges may be waiting
				// on merges to finish.
				System.Threading.Monitor.PulseAll(this);
				
				// It's possible we are called twice, eg if there was an
				// exception inside mergeInit
				if (merge.registerDone)
				{
					SegmentInfos sourceSegments = merge.segments;
					int end = sourceSegments.Count;
					for (int i = 0; i < end; i++)
						mergingSegments.Remove(sourceSegments.Info(i));
                    if(merge.info != null)
					    mergingSegments.Remove(merge.info);
					merge.registerDone = false;
				}
				
				runningMerges.Remove(merge);
			}
		}
		
        private void SetMergeDocStoreIsCompoundFile(MergePolicy.OneMerge merge)
        {
            lock (this)
            {
                string mergeDocStoreSegment = merge.info.DocStoreSegment;
                if (mergeDocStoreSegment != null && !merge.info.DocStoreIsCompoundFile)
                {
                    int size = segmentInfos.Count;
                    for (int i = 0; i < size; i++)
                    {
                        SegmentInfo info = segmentInfos.Info(i);
                        string docStoreSegment = info.DocStoreSegment;
                        if (docStoreSegment != null &&
                            docStoreSegment.Equals(mergeDocStoreSegment) &&
                            info.DocStoreIsCompoundFile)
                        {
                            merge.info.DocStoreIsCompoundFile = true;
                            break;
                        }
                    }
                }
            }
        }

        private void CloseMergeReaders(MergePolicy.OneMerge merge, bool suppressExceptions, IState state)
        {
            lock (this)
            {
                int numSegments = merge.segments.Count;
                if (suppressExceptions)
                {
                    // Suppress any new exceptions so we throw the
                    // original cause
                    for (int i = 0; i < numSegments; i++)
                    {
                        if (merge.readers[i] != null)
                        {
                            try
                            {
                                readerPool.Release(merge.readers[i], false, state);
                            }
                            catch (Exception)
                            {
                            }
                            merge.readers[i] = null;
                        }

                        if (merge.readersClone[i] != null)
                        {
                            try
                            {
                                merge.readersClone[i].Close();
                            }
                            catch (Exception)
                            {
                            }
                            // This was a private clone and we had the
                            // only reference
                            System.Diagnostics.Debug.Assert(merge.readersClone[i].RefCount == 0); //: "refCount should be 0 but is " + merge.readersClone[i].getRefCount();
                            merge.readersClone[i] = null;
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < numSegments; i++)
                    {
                        if (merge.readers[i] != null)
                        {
                            readerPool.Release(merge.readers[i], true, state);
                            merge.readers[i] = null;
                        }

                        if (merge.readersClone[i] != null)
                        {
                            merge.readersClone[i].Close();
                            // This was a private clone and we had the only reference
                            System.Diagnostics.Debug.Assert(merge.readersClone[i].RefCount == 0);
                            merge.readersClone[i] = null;
                        }
                    }
                }
            }
        }


		/// <summary>Does the actual (time-consuming) work of the merge,
		/// but without holding synchronized lock on IndexWriter
		/// instance 
		/// </summary>
		private int MergeMiddle(MergePolicy.OneMerge merge, IState state)
		{
			
			merge.CheckAborted(directory, state);
			
			System.String mergedName = merge.info.name;
			
			SegmentMerger merger = null;
			
			int mergedDocCount = 0;
			
			SegmentInfos sourceSegments = merge.segments;
			int numSegments = sourceSegments.Count;
			
			if (infoStream != null)
				Message("merging " + merge.SegString(directory, state));
			
			merger = new SegmentMerger(this, mergedName, merge);
			
			merge.readers = new SegmentReader[numSegments];
			merge.readersClone = new SegmentReader[numSegments];
			
			bool mergeDocStores = false;

            String currentDocStoreSegment;
            lock(this) {
                currentDocStoreSegment = docWriter.DocStoreSegment;
            }
            bool currentDSSMerged = false;

			// This is try/finally to make sure merger's readers are
			// closed:
			bool success = false;
            try
            {
                int totDocCount = 0;

                for (int i = 0; i < numSegments; i++)
                {

                    SegmentInfo info = sourceSegments.Info(i);

                    // Hold onto the "live" reader; we will use this to
                    // commit merged deletes
                    SegmentReader reader = merge.readers[i] = readerPool.Get(info, merge.mergeDocStores, MERGE_READ_BUFFER_SIZE, -1, state);

                    // We clone the segment readers because other
                    // deletes may come in while we're merging so we
                    // need readers that will not change
                    SegmentReader clone = merge.readersClone[i] = (SegmentReader)reader.Clone(true, state);
                    merger.Add(clone);

                    if (clone.HasDeletions)
                    {
                        mergeDocStores = true;
                    }

                    if (info.DocStoreOffset != -1 && currentDocStoreSegment != null)
                    {
                        currentDSSMerged |= currentDocStoreSegment.Equals(info.DocStoreSegment);
                    }

                    totDocCount += clone.NumDocs();
                }

                if (infoStream != null)
                {
                    Message("merge: total " + totDocCount + " docs");
                }

                merge.CheckAborted(directory, state);

                // If deletions have arrived and it has now become
                // necessary to merge doc stores, go and open them:
                if (mergeDocStores && !merge.mergeDocStores)
                {
                    merge.mergeDocStores = true;
                    lock (this)
                    {
                        if (currentDSSMerged)
                        {
                            if (infoStream != null)
                            {
                                Message("now flush at mergeMiddle");
                            }
                            DoFlush(true, false, state);
                        }
                    }

                    for (int i = 0; i < numSegments; i++)
                    {
                        merge.readersClone[i].OpenDocStores(state);
                    }

                    // Clear DSS
                    merge.info.SetDocStore(-1, null, false);

                }

                // This is where all the work happens:
                mergedDocCount = merge.info.docCount = merger.Merge(merge.mergeDocStores, state);

                System.Diagnostics.Debug.Assert(mergedDocCount == totDocCount);

                if (merge.useCompoundFile)
                {

                    success = false;
                    string compoundFileName = IndexFileNames.SegmentFileName(mergedName, IndexFileNames.COMPOUND_FILE_EXTENSION);

                    try
                    {
                        if (infoStream != null)
                        {
                            Message("create compound file " + compoundFileName);
                        }
                        merger.CreateCompoundFile(compoundFileName);
                        success = true;
                    }
                    catch (System.IO.IOException ioe)
                    {
                        lock (this)
                        {
                            if (merge.IsAborted())
                            {
                                // This can happen if rollback or close(false)
                                // is called -- fall through to logic below to
                                // remove the partially created CFS:
                            }
                            else
                            {
                                HandleMergeException(ioe, merge, state);
                            }
                        }
                    }
                    catch (Exception t)
                    {
                        HandleMergeException(t, merge, state);
                    }
                    finally
                    {
                        if (!success)
                        {
                            if (infoStream != null)
                            {
                                Message("hit exception creating compound file during merge");
                            }

                            lock (this)
                            {
                                deleter.DeleteFile(compoundFileName, state);
                                deleter.DeleteNewFiles(merger.GetMergedFiles(), state);
                            }
                        }
                    }

                    success = false;

                    lock (this)
                    {

                        // delete new non cfs files directly: they were never
                        // registered with IFD
                        deleter.DeleteNewFiles(merger.GetMergedFiles(), state);

                        if (merge.IsAborted())
                        {
                            if (infoStream != null)
                            {
                                Message("abort merge after building CFS");
                            }
                            deleter.DeleteFile(compoundFileName, state);
                            return 0;
                        }
                    }

                    merge.info.SetUseCompoundFile(true);
                }

                int termsIndexDivisor;
                bool loadDocStores;

                // if the merged segment warmer was not installed when
                // this merge was started, causing us to not force
                // the docStores to close, we can't warm it now
                bool canWarm = merge.info.DocStoreSegment == null || currentDocStoreSegment == null || !merge.info.DocStoreSegment.Equals(currentDocStoreSegment);

                if (poolReaders && mergedSegmentWarmer != null && canWarm)
                {
                    // Load terms index & doc stores so the segment
                    // warmer can run searches, load documents/term
                    // vectors
                    termsIndexDivisor = readerTermsIndexDivisor;
                    loadDocStores = true;
                }
                else
                {
                    termsIndexDivisor = -1;
                    loadDocStores = false;
                }

                // TODO: in the non-realtime case, we may want to only
                // keep deletes (it's costly to open entire reader
                // when we just need deletes)

                SegmentReader mergedReader = readerPool.Get(merge.info, loadDocStores, BufferedIndexInput.BUFFER_SIZE, termsIndexDivisor, state);
                try
                {
                    if (poolReaders && mergedSegmentWarmer != null)
                    {
                        mergedSegmentWarmer.Warm(mergedReader);
                    }
                    if (!CommitMerge(merge, merger, mergedDocCount, mergedReader, state))
                    {
                        // commitMerge will return false if this merge was aborted
                        return 0;
                    }
                }
                finally
                {
                    lock (this)
                    {
                        readerPool.Release(mergedReader, state);
                    }
                }

                success = true;
            }
            finally
            {
                // Readers are already closed in commitMerge if we didn't hit
                // an exc:
                if (!success)
                {
                    CloseMergeReaders(merge, true, state);
                }
            }

			return mergedDocCount;
		}
		
		internal virtual void  AddMergeException(MergePolicy.OneMerge merge)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(merge.GetException() != null);
				if (!mergeExceptions.Contains(merge) && mergeGen == merge.mergeGen)
					mergeExceptions.Add(merge);
			}
		}
		
		// Apply buffered deletes to all segments.
		protected virtual bool ApplyDeletes(IState state)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(TestPoint("startApplyDeletes"));
                flushDeletesCount++;
				
				bool success = false;
				bool changed;
				try
				{
					changed = docWriter.ApplyDeletes(segmentInfos, state);
					success = true;
				}
				finally
				{
                    if (!success && infoStream != null)
                    {
                        Message("hit exception flushing deletes");
                    }
				}
				
				if (changed)
					Checkpoint(state);
				return changed;
			}
		}
		
		// For test purposes.
		internal int GetBufferedDeleteTermsSize()
		{
			lock (this)
			{
				return docWriter.GetBufferedDeleteTerms().Count;
			}
		}
		
		// For test purposes.
		internal int GetNumBufferedDeleteTerms()
		{
			lock (this)
			{
				return docWriter.GetNumBufferedDeleteTerms();
			}
		}
		
		// utility routines for tests
		public /*internal*/ virtual SegmentInfo NewestSegment()
		{
            return segmentInfos.Count > 0 ? segmentInfos.Info(segmentInfos.Count - 1) : null;
		}
		
		public virtual System.String SegString(IState state)
		{
			lock (this)
			{
				return SegString(segmentInfos, state);
			}
		}
		
		private System.String SegString(SegmentInfos infos, IState state)
		{
			lock (this)
			{
				System.Text.StringBuilder buffer = new System.Text.StringBuilder();
				int count = infos.Count;
				for (int i = 0; i < count; i++)
				{
					if (i > 0)
					{
						buffer.Append(' ');
					}
					SegmentInfo info = infos.Info(i);
					buffer.Append(info.SegString(directory, state));
					if (info.dir != directory)
						buffer.Append("**");
				}
				return buffer.ToString();
			}
		}
		
		// Files that have been sync'd already
        private HashSet<string> synced = new HashSet<string>();
		
		// Files that are now being sync'd
        private HashSet<string> syncing = new HashSet<string>();
		
		private bool StartSync(System.String fileName, ICollection<string> pending)
		{
			lock (synced)
			{
				if (!synced.Contains(fileName))
				{
					if (!syncing.Contains(fileName))
					{
						syncing.Add(fileName);
						return true;
					}
					else
					{
						pending.Add(fileName);
						return false;
					}
				}
				else
					return false;
			}
		}
		
		private void  FinishSync(System.String fileName, bool success)
		{
			lock (synced)
			{
				System.Diagnostics.Debug.Assert(syncing.Contains(fileName));
				syncing.Remove(fileName);
				if (success)
                    synced.Add(fileName);
				System.Threading.Monitor.PulseAll(synced);
			}
		}
		
		/// <summary>Blocks until all files in syncing are sync'd </summary>
		private bool WaitForAllSynced(ICollection<System.String> syncing)
		{
			lock (synced)
			{
				IEnumerator<string> it = syncing.GetEnumerator();
				while (it.MoveNext())
				{
					System.String fileName = it.Current;
					while (!synced.Contains(fileName))
					{
						if (!syncing.Contains(fileName))
						// There was an error because a file that was
						// previously syncing failed to appear in synced
							return false;
						else
							System.Threading.Monitor.Wait(synced);
							
					}
				}
				return true;
			}
		}
		
		private void  DoWait()
		{
			lock (this)
			{
				// NOTE: the callers of this method should in theory
				// be able to do simply wait(), but, as a defense
				// against thread timing hazards where notifyAll()
				// falls to be called, we wait for at most 1 second
				// and then return so caller can check if wait
				// conditions are satisified:
				System.Threading.Monitor.Wait(this, TimeSpan.FromMilliseconds(1000));
				
			}
		}
		
		/// <summary>Walk through all files referenced by the current
		/// segmentInfos and ask the Directory to sync each file,
		/// if it wasn't already.  If that succeeds, then we
		/// prepare a new segments_N file but do not fully commit
		/// it. 
		/// </summary>
        private void StartCommit(long sizeInBytes, IDictionary<string, string> commitUserData, IState state)
		{
			
			System.Diagnostics.Debug.Assert(TestPoint("startStartCommit"));

            // TODO: as of LUCENE-2095, we can simplify this method,
            // since only 1 thread can be in here at once
			
			if (hitOOM)
			{
				throw new System.SystemException("this writer hit an OutOfMemoryError; cannot commit");
			}
			
			try
			{
				
				if (infoStream != null)
					Message("startCommit(): start sizeInBytes=" + sizeInBytes);
				
				SegmentInfos toSync = null;
				long myChangeCount;
				
				lock (this)
				{
					// Wait for any running addIndexes to complete
					// first, then block any from running until we've
					// copied the segmentInfos we intend to sync:
					BlockAddIndexes(false);
					
					// On commit the segmentInfos must never
					// reference a segment in another directory:
					System.Diagnostics.Debug.Assert(!HasExternalSegments());
					
					try
					{
						
						System.Diagnostics.Debug.Assert(lastCommitChangeCount <= changeCount);
                        myChangeCount = changeCount;
						
						if (changeCount == lastCommitChangeCount)
						{
							if (infoStream != null)
								Message("  skip startCommit(): no changes pending");
							return ;
						}
						
						// First, we clone & incref the segmentInfos we intend
						// to sync, then, without locking, we sync() each file
						// referenced by toSync, in the background.  Multiple
						// threads can be doing this at once, if say a large
						// merge and a small merge finish at the same time:
						
						if (infoStream != null)
							Message("startCommit index=" + SegString(segmentInfos, state) + " changeCount=" + changeCount);

                        readerPool.Commit(state);
						
						// It's possible another flush (that did not close
                        // the open do stores) snuck in after the flush we
                        // just did, so we remove any tail segments
                        // referencing the open doc store from the
                        // SegmentInfos we are about to sync (the main
                        // SegmentInfos will keep them):
                        toSync = (SegmentInfos) segmentInfos.Clone();
                        string dss = docWriter.DocStoreSegment;
                        if (dss != null)
                        {
                            while (true)
                            {
                                String dss2 = toSync.Info(toSync.Count - 1).DocStoreSegment;
                                if (dss2 == null || !dss2.Equals(dss))
                                {
                                    break;
                                }
                                toSync.RemoveAt(toSync.Count - 1);
                                changeCount++;
                            }
                        }
						
						if (commitUserData != null)
							toSync.UserData = commitUserData;
						
						deleter.IncRef(toSync, false, state);
												
						ICollection<string> files = toSync.Files(directory, false, state);
						foreach(string fileName in files)
						{
							System.Diagnostics.Debug.Assert(directory.FileExists(fileName, state), "file " + fileName + " does not exist");
                            // If this trips it means we are missing a call to
                            // .checkpoint somewhere, because by the time we
                            // are called, deleter should know about every
                            // file referenced by the current head
                            // segmentInfos:
                            System.Diagnostics.Debug.Assert(deleter.Exists(fileName));
						}
					}
					finally
					{
						ResumeAddIndexes();
					}
				}
				
				System.Diagnostics.Debug.Assert(TestPoint("midStartCommit"));
				
				bool setPending = false;
				
				try
				{
					// Loop until all files toSync references are sync'd:
					while (true)
					{
                        ICollection<string> pending = new List<string>();
						
						IEnumerator<string> it = toSync.Files(directory, false, state).GetEnumerator();
						while (it.MoveNext())
						{
                            string fileName = it.Current;
							if (StartSync(fileName, pending))
							{
								bool success = false;
								try
								{
									// Because we incRef'd this commit point, above,
									// the file had better exist:
									System.Diagnostics.Debug.Assert(directory.FileExists(fileName, state), "file '" + fileName + "' does not exist dir=" + directory);
									if (infoStream != null)
										Message("now sync " + fileName);
									directory.Sync(fileName);
									success = true;
								}
								finally
								{
									FinishSync(fileName, success);
								}
							}
						}
						
						// All files that I require are either synced or being
						// synced by other threads.  If they are being synced,
						// we must at this point block until they are done.
						// If this returns false, that means an error in
						// another thread resulted in failing to actually
						// sync one of our files, so we repeat:
						if (WaitForAllSynced(pending))
							break;
					}
					
					System.Diagnostics.Debug.Assert(TestPoint("midStartCommit2"));
					
					lock (this)
					{
						// If someone saved a newer version of segments file
						// since I first started syncing my version, I can
						// safely skip saving myself since I've been
						// superseded:
						
						while (true)
						{
							if (myChangeCount <= lastCommitChangeCount)
							{
								if (infoStream != null)
								{
									Message("sync superseded by newer infos");
								}
								break;
							}
							else if (pendingCommit == null)
							{
								// My turn to commit
								
								if (segmentInfos.Generation > toSync.Generation)
									toSync.UpdateGeneration(segmentInfos);
								
								bool success = false;
								try
								{
									
									// Exception here means nothing is prepared
									// (this method unwinds everything it did on
									// an exception)
									try
									{
										toSync.PrepareCommit(directory, state);
									}
									finally
									{
										// Have our master segmentInfos record the
										// generations we just prepared.  We do this
										// on error or success so we don't
										// double-write a segments_N file.
										segmentInfos.UpdateGeneration(toSync);
									}
									
									System.Diagnostics.Debug.Assert(pendingCommit == null);
									setPending = true;
									pendingCommit = toSync;
									pendingCommitChangeCount = (uint) myChangeCount;
									success = true;
								}
								finally
								{
									if (!success && infoStream != null)
										Message("hit exception committing segments file");
								}
								break;
							}
							else
							{
								// Must wait for other commit to complete
								DoWait();
							}
						}
					}
					
					if (infoStream != null)
						Message("done all syncs");
					
					System.Diagnostics.Debug.Assert(TestPoint("midStartCommitSuccess"));
				}
				finally
				{
					lock (this)
					{
						if (!setPending)
							deleter.DecRef(toSync, state);
					}
				}
			}
			catch (System.OutOfMemoryException oom)
			{
				HandleOOM(oom, "startCommit");
			}
			System.Diagnostics.Debug.Assert(TestPoint("finishStartCommit"));
		}
		
		/// <summary> Returns <c>true</c> iff the index in the named directory is
		/// currently locked.
		/// </summary>
		/// <param name="directory">the directory to check for a lock
		/// </param>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public static bool IsLocked(Directory directory)
		{
			return directory.MakeLock(WRITE_LOCK_NAME).IsLocked();
		}
		
		/// <summary> Forcibly unlocks the index in the named directory.
		/// <p/>
		/// Caution: this should only be used by failure recovery code,
		/// when it is known that no other process nor thread is in fact
		/// currently accessing this index.
		/// </summary>
		public static void  Unlock(Directory directory)
		{
			directory.MakeLock(IndexWriter.WRITE_LOCK_NAME).Release();
		}
		
		/// <summary> Specifies maximum field length (in number of tokens/terms) in <see cref="IndexWriter" /> constructors.
		/// <see cref="SetMaxFieldLength(int)" /> overrides the value set by
		/// the constructor.
		/// </summary>
		public sealed class MaxFieldLength
		{
			
			private int limit;
			private System.String name;
			
			/// <summary> Private type-safe-enum-pattern constructor.
			/// 
			/// </summary>
			/// <param name="name">instance name
			/// </param>
			/// <param name="limit">maximum field length
			/// </param>
			internal MaxFieldLength(System.String name, int limit)
			{
				this.name = name;
				this.limit = limit;
			}
			
			/// <summary> Public constructor to allow users to specify the maximum field size limit.
			/// 
			/// </summary>
			/// <param name="limit">The maximum field length
			/// </param>
			public MaxFieldLength(int limit):this("User-specified", limit)
			{
			}

		    public int Limit
		    {
		        get { return limit; }
		    }

		    public override System.String ToString()
			{
				return name + ":" + limit;
			}
			
			/// <summary>Sets the maximum field length to <see cref="int.MaxValue" />. </summary>
			public static readonly MaxFieldLength UNLIMITED = new MaxFieldLength("UNLIMITED", System.Int32.MaxValue);
			
			/// <summary>  Sets the maximum field length to 
			/// <see cref="DEFAULT_MAX_FIELD_LENGTH" /> 
			/// 
			/// </summary>
			public static readonly MaxFieldLength LIMITED;
			static MaxFieldLength()
			{
				LIMITED = new MaxFieldLength("LIMITED", Lucene.Net.Index.IndexWriter.DEFAULT_MAX_FIELD_LENGTH);
			}
		}
		
		/// <summary>If <see cref="GetReader()" /> has been called (ie, this writer
		/// is in near real-time mode), then after a merge
		/// completes, this class can be invoked to warm the
		/// reader on the newly merged segment, before the merge
		/// commits.  This is not required for near real-time
		/// search, but will reduce search latency on opening a
		/// new near real-time reader after a merge completes.
		/// 
		/// <p/><b>NOTE:</b> This API is experimental and might
		/// change in incompatible ways in the next release.<p/>
		/// 
		/// <p/><b>NOTE</b>: warm is called before any deletes have
		/// been carried over to the merged segment. 
		/// </summary>
		public abstract class IndexReaderWarmer
		{
			public abstract void  Warm(IndexReader reader);
		}
		
		private IndexReaderWarmer mergedSegmentWarmer;

	    /// <summary>Gets or sets the merged segment warmer.  See <see cref="IndexReaderWarmer" />
	    ///. 
	    /// </summary>
	    public virtual IndexReaderWarmer MergedSegmentWarmer
	    {
	        set { mergedSegmentWarmer = value; }
	        get { return mergedSegmentWarmer; }
	    }

	    private void  HandleOOM(System.OutOfMemoryException oom, System.String location)
		{
			if (infoStream != null)
			{
				Message("hit OutOfMemoryError inside " + location);
			}
			hitOOM = true;

            var exceptionDispatchInfo = ExceptionDispatchInfo.Capture(oom);
            exceptionDispatchInfo.Throw();
		}
		
		// Used only by assert for testing.  Current points:
		//   startDoFlush
		//   startCommitMerge
		//   startStartCommit
		//   midStartCommit
		//   midStartCommit2
		//   midStartCommitSuccess
		//   finishStartCommit
		//   startCommitMergeDeletes
		//   startMergeInit
		//   startApplyDeletes
		//   DocumentsWriter.ThreadState.init start
		public /*internal*/ virtual bool TestPoint(System.String name)
		{
			return true;
		}
		
		internal virtual bool NrtIsCurrent(SegmentInfos infos)
		{
			lock (this)
			{
				if (!infos.Equals(segmentInfos))
				{
					// if any structural changes (new segments), we are
					// stale
					return false;
                }
                else if (infos.Generation != segmentInfos.Generation)
                {
                    // if any commit took place since we were opened, we
                    // are stale
                    return false;
                }
                else
                {
                    return !docWriter.AnyChanges;
                }
			}
		}
		
		internal virtual bool IsClosed()
		{
			lock (this)
			{
				return closed;
			}
		}

		static IndexWriter()
		{
			MAX_TERM_LENGTH = DocumentsWriter.MAX_TERM_LENGTH;
		}
	}

	/// <summary>
	/// Scope for Optimize function. This is also use to persist reference to CancellationToken because it is easly accessible from Mergers.
	/// </summary>
	public class OptimizeScope : IDisposable
	{
		public bool IsDisposed;
		public bool IsRunning;
		public readonly CancellationToken Token;

		public OptimizeScope(CancellationToken token)
		{
			Token = token;
			IsRunning = true;
			IsDisposed = false;
		}

		public void Dispose()
		{
			IsRunning = false;
			IsDisposed = true;
		}
	}
}
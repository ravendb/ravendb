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
using Lucene.Net.Documents;
using Document = Lucene.Net.Documents.Document;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Similarity = Lucene.Net.Search.Similarity;

namespace Lucene.Net.Index
{
	
	/// <summary>IndexReader is an abstract class, providing an interface for accessing an
	/// index.  Search of an index is done entirely through this abstract interface,
	/// so that any subclass which implements it is searchable.
	/// <p/> Concrete subclasses of IndexReader are usually constructed with a call to
	/// one of the static <c>open()</c> methods, e.g. <see cref="Open(Lucene.Net.Store.Directory, bool)" />
	///.
	/// <p/> For efficiency, in this API documents are often referred to via
	/// <i>document numbers</i>, non-negative integers which each name a unique
	/// document in the index.  These document numbers are ephemeral--they may change
	/// as documents are added to and deleted from an index.  Clients should thus not
	/// rely on a given document having the same number between sessions.
	/// <p/> An IndexReader can be opened on a directory for which an IndexWriter is
	/// opened already, but it cannot be used to delete documents from the index then.
	/// <p/>
	/// <b>NOTE</b>: for backwards API compatibility, several methods are not listed 
	/// as abstract, but have no useful implementations in this base class and 
	/// instead always throw UnsupportedOperationException.  Subclasses are 
	/// strongly encouraged to override these methods, but in many cases may not 
	/// need to.
	/// <p/>
	/// <p/>
	/// <b>NOTE</b>: as of 2.4, it's possible to open a read-only
	/// IndexReader using the static open methods that accepts the
	/// boolean readOnly parameter.  Such a reader has better
	/// better concurrency as it's not necessary to synchronize on the
	/// isDeleted method.  You must explicitly specify false
	/// if you want to make changes with the resulting IndexReader.
	/// <p/>
	/// <a name="thread-safety"></a><p/><b>NOTE</b>: <see cref="IndexReader" />
	/// instances are completely thread
	/// safe, meaning multiple threads can call any of its methods,
	/// concurrently.  If your application requires external
	/// synchronization, you should <b>not</b> synchronize on the
	/// <c>IndexReader</c> instance; use your own
	/// (non-Lucene) objects instead.
	/// </summary>
	public abstract class IndexReader : ILuceneCloneable, System.IDisposable
	{
		private class AnonymousClassFindSegmentsFile : SegmentInfos.FindSegmentsFile
		{
			private void  InitBlock(Lucene.Net.Store.Directory directory2)
			{
				this.directory2 = directory2;
			}
			private Lucene.Net.Store.Directory directory2;
			internal AnonymousClassFindSegmentsFile(Lucene.Net.Store.Directory directory2, Lucene.Net.Store.Directory Param1):base(Param1)
			{
				InitBlock(directory2);
			}
			public override System.Object DoBody(System.String segmentFileName, IState state)
			{
				return (long) directory2.FileModified(segmentFileName, state);
			}
		}
		
		/// <summary> Constants describing field properties, for example used for
		/// <see cref="IndexReader.GetFieldNames(FieldOption)" />.
		/// </summary>
		public sealed class FieldOption
		{
			private readonly System.String option;
			internal FieldOption()
			{
			}
			internal FieldOption(System.String option)
			{
				this.option = option;
			}
			public override System.String ToString()
			{
				return this.option;
			}
			/// <summary>All fields </summary>
			public static readonly FieldOption ALL = new FieldOption("ALL");
			/// <summary>All indexed fields </summary>
			public static readonly FieldOption INDEXED = new FieldOption("INDEXED");
			/// <summary>All fields that store payloads </summary>
			public static readonly FieldOption STORES_PAYLOADS = new FieldOption("STORES_PAYLOADS");
			/// <summary>All fields that omit tf </summary>
			public static readonly FieldOption OMIT_TERM_FREQ_AND_POSITIONS = new FieldOption("OMIT_TERM_FREQ_AND_POSITIONS");
			/// <summary>All fields which are not indexed </summary>
			public static readonly FieldOption UNINDEXED = new FieldOption("UNINDEXED");
			/// <summary>All fields which are indexed with termvectors enabled </summary>
			public static readonly FieldOption INDEXED_WITH_TERMVECTOR = new FieldOption("INDEXED_WITH_TERMVECTOR");
			/// <summary>All fields which are indexed but don't have termvectors enabled </summary>
			public static readonly FieldOption INDEXED_NO_TERMVECTOR = new FieldOption("INDEXED_NO_TERMVECTOR");
			/// <summary>All fields with termvectors enabled. Please note that only standard termvector fields are returned </summary>
			public static readonly FieldOption TERMVECTOR = new FieldOption("TERMVECTOR");
			/// <summary>All fields with termvectors with position values enabled </summary>
			public static readonly FieldOption TERMVECTOR_WITH_POSITION = new FieldOption("TERMVECTOR_WITH_POSITION");
			/// <summary>All fields with termvectors with offset values enabled </summary>
			public static readonly FieldOption TERMVECTOR_WITH_OFFSET = new FieldOption("TERMVECTOR_WITH_OFFSET");
			/// <summary>All fields with termvectors with offset values and position values enabled </summary>
			public static readonly FieldOption TERMVECTOR_WITH_POSITION_OFFSET = new FieldOption("TERMVECTOR_WITH_POSITION_OFFSET");
		}
		
		private bool closed;
		protected internal bool hasChanges;
		
		private int refCount;

		protected internal static int DEFAULT_TERMS_INDEX_DIVISOR = 1;

	    /// <summary>Expert: returns the current refCount for this reader </summary>
	    public virtual int RefCount
	    {
	        get
	        {
	            lock (this)
	            {
	                return refCount;
	            }
	        }
	    }

	    /// <summary> Expert: increments the refCount of this IndexReader
		/// instance.  RefCounts are used to determine when a
		/// reader can be closed safely, i.e. as soon as there are
		/// no more references.  Be sure to always call a
		/// corresponding <see cref="DecRef" />, in a finally clause;
		/// otherwise the reader may never be closed.  Note that
		/// <see cref="Close" /> simply calls decRef(), which means that
		/// the IndexReader will not really be closed until <see cref="DecRef" />
		/// has been called for all outstanding
		/// references.
		/// 
		/// </summary>
		/// <seealso cref="DecRef">
		/// </seealso>
		public virtual void  IncRef()
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(refCount > 0);
				EnsureOpen();
				refCount++;
			}
		}
		
		/// <summary> Expert: decreases the refCount of this IndexReader
		/// instance.  If the refCount drops to 0, then pending
		/// changes (if any) are committed to the index and this
		/// reader is closed.
		/// 
		/// </summary>
		/// <throws>  IOException in case an IOException occurs in commit() or doClose() </throws>
		/// <summary> 
		/// </summary>
		/// <seealso cref="IncRef">
		/// </seealso>
		public virtual void  DecRef(IState state)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(refCount > 0);
				EnsureOpen();
				if (refCount == 1)
				{
					Commit(state);
					DoClose(state);
				}
				refCount--;
			}
		}
		
        public void CloseWithoutCommit()
        {
            lock (this)
            {
                if (!closed)
                {
                    System.Diagnostics.Debug.Assert(refCount > 0);
                    EnsureOpen();
                    if (refCount == 1)
                    {
                        DoClose(null);
                        closed = true;
                    }
                    refCount--;
                }
            }
        }

		protected internal IndexReader()
		{
			refCount = 1;
		}
		
		/// <throws>  AlreadyClosedException if this IndexReader is closed </throws>
        protected internal void EnsureOpen()
		{
		    if (refCount <= 0)
		    {
		        throw new AlreadyClosedException("this IndexReader is closed");
		    }
		}
		
		/// <summary>Returns an IndexReader reading the index in the given
		/// Directory.  You should pass readOnly=true, since it
		/// gives much better concurrent performance, unless you
		/// intend to do write operations (delete documents or
		/// change norms) with the reader.
		/// </summary>
		/// <param name="directory">the index directory</param>
        /// <param name="readOnly">true if no changes (deletions, norms) will be made with this IndexReader</param>
        /// <exception cref="CorruptIndexException">CorruptIndexException if the index is corrupt</exception>
        /// <exception cref="System.IO.IOException">IOException if there is a low-level IO error</exception>
		public static IndexReader Open(Directory directory, bool readOnly, IState state)
		{
			return Open(directory, null, null, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, state);
		}
		
		/// <summary>Expert: returns an IndexReader reading the index in the given
		/// <see cref="IndexCommit" />.  You should pass readOnly=true, since it
		/// gives much better concurrent performance, unless you
		/// intend to do write operations (delete documents or
		/// change norms) with the reader.
		/// </summary>
		/// <param name="commit">the commit point to open
		/// </param>
		/// <param name="readOnly">true if no changes (deletions, norms) will be made with this IndexReader
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public static IndexReader Open(IndexCommit commit, bool readOnly, IState state)
		{
			return Open(commit.Directory, null, commit, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, state);
		}
		
		/// <summary>Expert: returns an IndexReader reading the index in
		/// the given Directory, with a custom <see cref="IndexDeletionPolicy" />
		///.  You should pass readOnly=true,
		/// since it gives much better concurrent performance,
		/// unless you intend to do write operations (delete
		/// documents or change norms) with the reader.
		/// </summary>
		/// <param name="directory">the index directory
		/// </param>
		/// <param name="deletionPolicy">a custom deletion policy (only used
		/// if you use this reader to perform deletes or to set
		/// norms); see <see cref="IndexWriter" /> for details.
		/// </param>
		/// <param name="readOnly">true if no changes (deletions, norms) will be made with this IndexReader
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public static IndexReader Open(Directory directory, IndexDeletionPolicy deletionPolicy, bool readOnly, IState state)
		{
			return Open(directory, deletionPolicy, null, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, state);
		}

		/// <summary>Expert: returns an IndexReader reading the index in
		/// the given Directory, with a custom <see cref="IndexDeletionPolicy" />
		///.  You should pass readOnly=true,
		/// since it gives much better concurrent performance,
		/// unless you intend to do write operations (delete
		/// documents or change norms) with the reader.
		/// </summary>
		/// <param name="directory">the index directory
		/// </param>
		/// <param name="deletionPolicy">a custom deletion policy (only used
		/// if you use this reader to perform deletes or to set
		/// norms); see <see cref="IndexWriter" /> for details.
		/// </param>
		/// <param name="readOnly">true if no changes (deletions, norms) will be made with this IndexReader
		/// </param>
		/// <param name="termInfosIndexDivisor">Subsamples which indexed
		/// terms are loaded into RAM. This has the same effect as <see>
		///                                                          <cref>IndexWriter.SetTermIndexInterval</cref>
		///                                                        </see> except that setting
		/// must be done at indexing time while this setting can be
		/// set per reader.  When set to N, then one in every
		/// N*termIndexInterval terms in the index is loaded into
		/// memory.  By setting this to a value > 1 you can reduce
		/// memory usage, at the expense of higher latency when
		/// loading a TermInfo.  The default value is 1.  Set this
		/// to -1 to skip loading the terms index entirely.
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public static IndexReader Open(Directory directory, IndexDeletionPolicy deletionPolicy, bool readOnly, int termInfosIndexDivisor, IState state)
		{
			return Open(directory, deletionPolicy, null, readOnly, termInfosIndexDivisor, state);
		}
		
		/// <summary>Expert: returns an IndexReader reading the index in
		/// the given Directory, using a specific commit and with
		/// a custom <see cref="IndexDeletionPolicy" />.  You should pass
		/// readOnly=true, since it gives much better concurrent
		/// performance, unless you intend to do write operations
		/// (delete documents or change norms) with the reader.
		/// </summary>
		/// <param name="commit">the specific <see cref="IndexCommit" /> to open;
		/// see <see cref="IndexReader.ListCommits" /> to list all commits
		/// in a directory
		/// </param>
		/// <param name="deletionPolicy">a custom deletion policy (only used
		/// if you use this reader to perform deletes or to set
		/// norms); see <see cref="IndexWriter" /> for details.
		/// </param>
		/// <param name="readOnly">true if no changes (deletions, norms) will be made with this IndexReader
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public static IndexReader Open(IndexCommit commit, IndexDeletionPolicy deletionPolicy, bool readOnly, IState state)
		{
			return Open(commit.Directory, deletionPolicy, commit, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, state);
		}

		/// <summary>Expert: returns an IndexReader reading the index in
		/// the given Directory, using a specific commit and with
		/// a custom <see cref="IndexDeletionPolicy" />.  You should pass
		/// readOnly=true, since it gives much better concurrent
		/// performance, unless you intend to do write operations
		/// (delete documents or change norms) with the reader.
		/// </summary>
		/// <param name="commit">the specific <see cref="IndexCommit" /> to open;
		/// see <see cref="IndexReader.ListCommits" /> to list all commits
		/// in a directory
		/// </param>
		/// <param name="deletionPolicy">a custom deletion policy (only used
		/// if you use this reader to perform deletes or to set
		/// norms); see <see cref="IndexWriter" /> for details.
		/// </param>
		/// <param name="readOnly">true if no changes (deletions, norms) will be made with this IndexReader
		/// </param>
		/// <param name="termInfosIndexDivisor">Subsambles which indexed
		/// terms are loaded into RAM. This has the same effect as <see>
		///                                                          <cref>IndexWriter.SetTermIndexInterval</cref>
		///                                                        </see> except that setting
		/// must be done at indexing time while this setting can be
		/// set per reader.  When set to N, then one in every
		/// N*termIndexInterval terms in the index is loaded into
		/// memory.  By setting this to a value > 1 you can reduce
		/// memory usage, at the expense of higher latency when
		/// loading a TermInfo.  The default value is 1.  Set this
		/// to -1 to skip loading the terms index entirely.
		/// </param>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public static IndexReader Open(IndexCommit commit, IndexDeletionPolicy deletionPolicy, bool readOnly, int termInfosIndexDivisor, IState state)
		{
			return Open(commit.Directory, deletionPolicy, commit, readOnly, termInfosIndexDivisor, state);
		}
		
		private static IndexReader Open(Directory directory, IndexDeletionPolicy deletionPolicy, IndexCommit commit, bool readOnly, int termInfosIndexDivisor, IState state)
		{
			return DirectoryReader.Open(directory, deletionPolicy, commit, readOnly, termInfosIndexDivisor, state);
		}
		
		/// <summary> Refreshes an IndexReader if the index has changed since this instance 
		/// was (re)opened. 
		/// <p/>
		/// Opening an IndexReader is an expensive operation. This method can be used
		/// to refresh an existing IndexReader to reduce these costs. This method 
		/// tries to only load segments that have changed or were created after the 
		/// IndexReader was (re)opened.
		/// <p/>
		/// If the index has not changed since this instance was (re)opened, then this
		/// call is a NOOP and returns this instance. Otherwise, a new instance is 
		/// returned. The old instance is <b>not</b> closed and remains usable.<br/>
		/// <p/>   
		/// If the reader is reopened, even though they share
		/// resources internally, it's safe to make changes
		/// (deletions, norms) with the new reader.  All shared
		/// mutable state obeys "copy on write" semantics to ensure
		/// the changes are not seen by other readers.
		/// <p/>
		/// You can determine whether a reader was actually reopened by comparing the
		/// old instance with the instance returned by this method: 
        /// <code>
		/// IndexReader reader = ... 
		/// ...
		/// IndexReader newReader = r.reopen();
		/// if (newReader != reader) {
		/// ...     // reader was reopened
		/// reader.close(); 
		/// }
		/// reader = newReader;
		/// ...
        /// </code>
		/// 
		/// Be sure to synchronize that code so that other threads,
		/// if present, can never use reader after it has been
		/// closed and before it's switched to newReader.
		/// 
		/// <p/><b>NOTE</b>: If this reader is a near real-time
		/// reader (obtained from <see cref="IndexWriter.GetReader()" />,
		/// reopen() will simply call writer.getReader() again for
		/// you, though this may change in the future.
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual IndexReader Reopen(IState state)
		{
			lock (this)
			{
				throw new NotSupportedException("This reader does not support reopen().");
			}
		}
		
		
		/// <summary>Just like <see cref="Reopen()" />, except you can change the
		/// readOnly of the original reader.  If the index is
		/// unchanged but readOnly is different then a new reader
		/// will be returned. 
		/// </summary>
		public virtual IndexReader Reopen(bool openReadOnly, IState state)
		{
			lock (this)
			{
				throw new NotSupportedException("This reader does not support reopen().");
			}
		}
		
		/// <summary>Expert: reopen this reader on a specific commit point.
		/// This always returns a readOnly reader.  If the
		/// specified commit point matches what this reader is
		/// already on, and this reader is already readOnly, then
		/// this same instance is returned; if it is not already
		/// readOnly, a readOnly clone is returned. 
		/// </summary>
		public virtual IndexReader Reopen(IndexCommit commit, IState state)
		{
			lock (this)
			{
				throw new NotSupportedException("This reader does not support reopen(IndexCommit).");
			}
		}
		
		/// <summary> Efficiently clones the IndexReader (sharing most
		/// internal state).
		/// <p/>
		/// On cloning a reader with pending changes (deletions,
		/// norms), the original reader transfers its write lock to
		/// the cloned reader.  This means only the cloned reader
		/// may make further changes to the index, and commit the
		/// changes to the index on close, but the old reader still
		/// reflects all changes made up until it was cloned.
		/// <p/>
		/// Like <see cref="Reopen()" />, it's safe to make changes to
		/// either the original or the cloned reader: all shared
		/// mutable state obeys "copy on write" semantics to ensure
		/// the changes are not seen by other readers.
		/// <p/>
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual System.Object Clone(IState state)
		{
			throw new System.NotSupportedException("This reader does not implement clone()");
		}
		
		/// <summary> Clones the IndexReader and optionally changes readOnly.  A readOnly 
		/// reader cannot open a writeable reader.  
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual IndexReader Clone(bool openReadOnly, IState state)
		{
			lock (this)
			{
				throw new System.NotSupportedException("This reader does not implement clone()");
			}
		}
		
		/// <summary> Returns the directory associated with this index.  The Default 
		/// implementation returns the directory specified by subclasses when 
		/// delegating to the IndexReader(Directory) constructor, or throws an 
		/// UnsupportedOperationException if one was not specified.
		/// </summary>
		/// <throws>  UnsupportedOperationException if no directory </throws>
		public virtual Directory Directory()
		{
			EnsureOpen();
            throw new NotSupportedException("This reader does not support this method.");
		}
		
		/// <summary> Returns the time the index in the named directory was last modified. 
		/// Do not use this to check whether the reader is still up-to-date, use
		/// <see cref="IsCurrent()" /> instead. 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public static long LastModified(Directory directory2, IState state)
		{
			return (long) ((System.Int64) new AnonymousClassFindSegmentsFile(directory2, directory2).Run(state));
		}
		
		/// <summary> Reads version number from segments files. The version number is
		/// initialized with a timestamp and then increased by one for each change of
		/// the index.
		/// 
		/// </summary>
		/// <param name="directory">where the index resides.
		/// </param>
		/// <returns> version number.
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public static long GetCurrentVersion(Directory directory, IState state)
		{
			return SegmentInfos.ReadCurrentVersion(directory, state);
		}

        /// <summary> Reads commitUserData, previously passed to 
        /// <see cref="IndexWriter.Commit(System.Collections.Generic.IDictionary{string, string})" />,
		/// from current index segments file.  This will return null if 
        /// <see cref="IndexWriter.Commit(System.Collections.Generic.IDictionary{string, string})" />
		/// has never been called for this index.
		/// </summary>
		/// <param name="directory">where the index resides.
		/// </param>
		/// <returns> commit userData.
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		/// <summary> 
		/// </summary>
		/// <seealso cref="GetCommitUserData(Store.Directory)">
		/// </seealso>
        public static System.Collections.Generic.IDictionary<string, string> GetCommitUserData(Directory directory, IState state)
		{
			return SegmentInfos.ReadCurrentUserData(directory, state);
		}

	    /// <summary> Version number when this IndexReader was opened. Not implemented in the
	    /// IndexReader base class.
	    /// 
	    /// <p/>
	    /// If this reader is based on a Directory (ie, was created by calling
	    /// <see cref="Open(Lucene.Net.Store.Directory, bool)" />, or <see cref="Reopen()" /> 
	    /// on a reader based on a Directory), then
	    /// this method returns the version recorded in the commit that the reader
	    /// opened. This version is advanced every time <see cref="IndexWriter.Commit()" /> is
	    /// called.
	    /// <p/>
	    /// 
	    /// <p/>
	    /// If instead this reader is a near real-time reader (ie, obtained by a call
	    /// to <see cref="IndexWriter.GetReader()" />, or by calling <see cref="Reopen()" /> on a near
	    /// real-time reader), then this method returns the version of the last
	    /// commit done by the writer. Note that even as further changes are made
	    /// with the writer, the version will not changed until a commit is
	    /// completed. Thus, you should not rely on this method to determine when a
	    /// near real-time reader should be opened. Use <see cref="IsCurrent" /> instead.
	    /// <p/>
	    /// 
	    /// </summary>
	    /// <throws>  UnsupportedOperationException </throws>
	    /// <summary>             unless overridden in subclass
	    /// </summary>
	    public virtual long Version
	    {
	        get { throw new System.NotSupportedException("This reader does not support this method."); }
	    }

	    /// <summary> Retrieve the String userData optionally passed to
	    /// <see cref="IndexWriter.Commit(System.Collections.Generic.IDictionary{string, string})" />.  
	    /// This will return null if 
	    /// <see cref="IndexWriter.Commit(System.Collections.Generic.IDictionary{string, string})" />
	    /// has never been called for this index.
	    /// </summary>
	    /// <seealso cref="GetCommitUserData(Store.Directory)">
	    /// </seealso>
	    public virtual IDictionary<string, string> CommitUserData
	    {
	        get { throw new System.NotSupportedException("This reader does not support this method."); }
	    }

		/// <summary> Check whether any new changes have occurred to the index since this
		/// reader was opened.
		/// 
		/// <p/>
		/// If this reader is based on a Directory (ie, was created by calling
		/// <see>
		///   <cref>Open(Store.Directory)</cref>
		/// </see> , or <see cref="Reopen()" /> on a reader based on a Directory), then
		/// this method checks if any further commits (see <see cref="IndexWriter.Commit()" />
		/// have occurred in that directory).
		/// <p/>
		/// 
		/// <p/>
		/// If instead this reader is a near real-time reader (ie, obtained by a call
		/// to <see cref="IndexWriter.GetReader()" />, or by calling <see cref="Reopen()" /> on a near
		/// real-time reader), then this method checks if either a new commmit has
		/// occurred, or any new uncommitted changes have taken place via the writer.
		/// Note that even if the writer has only performed merging, this method will
		/// still return false.
		/// <p/>
		/// 
		/// <p/>
		/// In any event, if this returns false, you should call <see cref="Reopen()" /> to
		/// get a new reader that sees the changes.
		/// <p/>
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		/// <throws>  UnsupportedOperationException unless overridden in subclass </throws>
		public virtual bool IsCurrent(IState state)
	    {
	        throw new NotSupportedException("This reader does not support this method.");
	    }

	    /// <summary> Checks is the index is optimized (if it has a single segment and 
	    /// no deletions).  Not implemented in the IndexReader base class.
	    /// </summary>
	    /// <returns> &amp;lt;c&amp;gt;true&amp;lt;/c&amp;gt; if the index is optimized; &amp;lt;c&amp;gt;false&amp;lt;/c&amp;gt; otherwise </returns>
	    /// <throws>  UnsupportedOperationException unless overridden in subclass </throws>
	    public virtual bool IsOptimized()
	    {
	        throw new NotSupportedException("This reader does not support this method.");
	    }

	    /// <summary> Return an array of term frequency vectors for the specified document.
		/// The array contains a vector for each vectorized field in the document.
		/// Each vector contains terms and frequencies for all terms in a given vectorized field.
		/// If no such fields existed, the method returns null. The term vectors that are
		/// returned may either be of type <see cref="ITermFreqVector" />
		/// or of type <see cref="TermPositionVector" /> if
		/// positions or offsets have been stored.
		/// 
		/// </summary>
		/// <param name="docNumber">document for which term frequency vectors are returned
		/// </param>
		/// <returns> array of term frequency vectors. May be null if no term vectors have been
		/// stored for the specified document.
		/// </returns>
		/// <throws>  IOException if index cannot be accessed </throws>
		/// <seealso cref="Lucene.Net.Documents.Field.TermVector">
		/// </seealso>
		abstract public ITermFreqVector[] GetTermFreqVectors(int docNumber, IState state);
		
		
		/// <summary> Return a term frequency vector for the specified document and field. The
		/// returned vector contains terms and frequencies for the terms in
		/// the specified field of this document, if the field had the storeTermVector
		/// flag set. If termvectors had been stored with positions or offsets, a 
		/// <see cref="TermPositionVector" /> is returned.
		/// 
		/// </summary>
		/// <param name="docNumber">document for which the term frequency vector is returned
		/// </param>
		/// <param name="field">field for which the term frequency vector is returned.
		/// </param>
		/// <returns> term frequency vector May be null if field does not exist in the specified
		/// document or term vector was not stored.
		/// </returns>
		/// <throws>  IOException if index cannot be accessed </throws>
		/// <seealso cref="Lucene.Net.Documents.Field.TermVector">
		/// </seealso>
		abstract public ITermFreqVector GetTermFreqVector(int docNumber, String field, IState state);
		
		/// <summary> Load the Term Vector into a user-defined data structure instead of relying on the parallel arrays of
		/// the <see cref="ITermFreqVector" />.
		/// </summary>
		/// <param name="docNumber">The number of the document to load the vector for
		/// </param>
		/// <param name="field">The name of the field to load
		/// </param>
		/// <param name="mapper">The <see cref="TermVectorMapper" /> to process the vector.  Must not be null
		/// </param>
		/// <throws>  IOException if term vectors cannot be accessed or if they do not exist on the field and doc. specified. </throws>
		/// <summary> 
		/// </summary>
		abstract public void  GetTermFreqVector(int docNumber, String field, TermVectorMapper mapper, IState state);
		
		/// <summary> Map all the term vectors for all fields in a Document</summary>
		/// <param name="docNumber">The number of the document to load the vector for
		/// </param>
		/// <param name="mapper">The <see cref="TermVectorMapper" /> to process the vector.  Must not be null
		/// </param>
		/// <throws>  IOException if term vectors cannot be accessed or if they do not exist on the field and doc. specified. </throws>
		abstract public void  GetTermFreqVector(int docNumber, TermVectorMapper mapper, IState state);
		
		/// <summary> Returns <c>true</c> if an index exists at the specified directory.
		/// If the directory does not exist or if there is no index in it.
		/// </summary>
		/// <param name="directory">the directory to check for an index
		/// </param>
		/// <returns> <c>true</c> if an index exists; <c>false</c> otherwise
		/// </returns>
		/// <throws>  IOException if there is a problem with accessing the index </throws>
		public static bool IndexExists(Directory directory, IState state)
		{
			return SegmentInfos.GetCurrentSegmentGeneration(directory, state) != - 1;
		}

	    /// <summary>Returns the number of documents in this index. </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public abstract int NumDocs();

	    /// <summary>Returns one greater than the largest possible document number.
	    /// This may be used to, e.g., determine how big to allocate an array which
	    /// will have an element for every document number in an index.
	    /// </summary>
	    public abstract int MaxDoc { get; }

	    /// <summary>Returns the number of deleted documents. </summary>
	    public virtual int NumDeletedDocs
	    {
	        get { return MaxDoc - NumDocs(); }
	    }

	    /// <summary> Returns the stored fields of the <c>n</c><sup>th</sup>
		/// <c>Document</c> in this index.
		/// <p/>
		/// <b>NOTE:</b> for performance reasons, this method does not check if the
		/// requested document is deleted, and therefore asking for a deleted document
		/// may yield unspecified results. Usually this is not required, however you
		/// can call <see cref="IsDeleted(int)" /> with the requested document ID to verify
		/// the document is not deleted.
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual Document Document(int n, IState state)
		{
			EnsureOpen();
			return Document(n, null, state);
		}
		
		/// <summary> Get the <see cref="Lucene.Net.Documents.Document" /> at the <c>n</c>
		/// <sup>th</sup> position. The <see cref="FieldSelector" /> may be used to determine
		/// what <see cref="Lucene.Net.Documents.Field" />s to load and how they should
		/// be loaded. <b>NOTE:</b> If this Reader (more specifically, the underlying
		/// <c>FieldsReader</c>) is closed before the lazy
		/// <see cref="Lucene.Net.Documents.Field" /> is loaded an exception may be
		/// thrown. If you want the value of a lazy
		/// <see cref="Lucene.Net.Documents.Field" /> to be available after closing you
		/// must explicitly load it or fetch the Document again with a new loader.
		/// <p/>
		/// <b>NOTE:</b> for performance reasons, this method does not check if the
		/// requested document is deleted, and therefore asking for a deleted document
		/// may yield unspecified results. Usually this is not required, however you
		/// can call <see cref="IsDeleted(int)" /> with the requested document ID to verify
		/// the document is not deleted.
		/// 
		/// </summary>
		/// <param name="n">Get the document at the <c>n</c><sup>th</sup> position
		/// </param>
		/// <param name="fieldSelector">The <see cref="FieldSelector" /> to use to determine what
		/// Fields should be loaded on the Document. May be null, in which case
		/// all Fields will be loaded.
		/// </param>
		/// <returns> The stored fields of the
		/// <see cref="Lucene.Net.Documents.Document" /> at the nth position
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		/// <seealso cref="IFieldable">
		/// </seealso>
		/// <seealso cref="Lucene.Net.Documents.FieldSelector">
		/// </seealso>
		/// <seealso cref="Lucene.Net.Documents.SetBasedFieldSelector">
		/// </seealso>
		/// <seealso cref="Lucene.Net.Documents.LoadFirstFieldSelector">
		/// </seealso>
		// TODO (1.5): When we convert to JDK 1.5 make this Set<String>
		public abstract Document Document(int n, FieldSelector fieldSelector, IState state);
		
		/// <summary>Returns true if document <i>n</i> has been deleted </summary>
		public abstract bool IsDeleted(int n);

	    /// <summary>Returns true if any documents have been deleted </summary>
	    public abstract bool HasDeletions { get; }

	    /// <summary>Returns true if there are norms stored for this field. </summary>
		public virtual bool HasNorms(System.String field, IState state)
		{
			// backward compatible implementation.
			// SegmentReader has an efficient implementation.
			EnsureOpen();
			return Norms(field, state) != null;
		}

		/// <summary>
		/// Returns the byte-encoded normalization factor for the named field of
		/// every document.  This is used by the search code to score documents.
		/// </summary>
        /// <seealso cref="Lucene.Net.Documents.AbstractField.Boost" />
		public abstract byte[] Norms(System.String field, IState state);
		
		/// <summary>
		/// Reads the byte-encoded normalization factor for the named field of every
		/// document.  This is used by the search code to score documents.
		/// </summary>
		/// <seealso cref="Lucene.Net.Documents.AbstractField.Boost" />
		public abstract void  Norms(System.String field, byte[] bytes, int offset, IState state);
		
		/// <summary>Expert: Resets the normalization factor for the named field of the named
		/// document.  The norm represents the product of the field's <see cref="IFieldable.Boost">boost</see>
        /// and its <see cref="Similarity.LengthNorm(String,int)">length normalization</see>.  Thus, to preserve the length normalization
		/// values when resetting this, one should base the new value upon the old.
		/// 
		/// <b>NOTE:</b> If this field does not store norms, then
		/// this method call will silently do nothing.
		/// </summary>
		/// <seealso cref="Norms(String)" />
		/// <seealso cref="Similarity.DecodeNorm(byte)" />
		/// <exception cref="StaleReaderException">
        /// If the index has changed since this reader was opened
		/// </exception>
        /// <exception cref="CorruptIndexException">
        /// If the index is corrupt
		/// </exception>
		/// <exception cref="LockObtainFailedException">
        /// If another writer has this index open (<c>write.lock</c> could not be obtained)
		/// </exception>
		/// <exception cref="System.IO.IOException">
        /// If there is a low-level IO error
		/// </exception>
		public virtual void  SetNorm(int doc, String field, byte value, IState state)
		{
			lock (this)
			{
				EnsureOpen();
				AcquireWriteLock(state);
				hasChanges = true;
				DoSetNorm(doc, field, value, state);
			}
		}
		
		/// <summary>Implements setNorm in subclass.</summary>
		protected internal abstract void  DoSetNorm(int doc, System.String field, byte value_Renamed, IState state);
		
		/// <summary>
		/// Expert: Resets the normalization factor for the named field of the named document.
		/// </summary>
		/// <seealso cref="Norms(String)" />
        /// <seealso cref="Similarity.DecodeNorm(byte)" />
        /// <exception cref="StaleReaderException">
        /// If the index has changed since this reader was opened
        /// </exception>
        /// <exception cref="CorruptIndexException">
        /// If the index is corrupt
        /// </exception>
        /// <exception cref="LockObtainFailedException">
        /// If another writer has this index open (<c>write.lock</c> could not be obtained)
        /// </exception>
        /// <exception cref="System.IO.IOException">
        /// If there is a low-level IO error
        /// </exception>
		public virtual void  SetNorm(int doc, string field, float value, IState state)
		{
			EnsureOpen();
			SetNorm(doc, field, Similarity.EncodeNorm(value), state);
		}
		
		/// <summary>Returns an enumeration of all the terms in the index. The
		/// enumeration is ordered by Term.compareTo(). Each term is greater
		/// than all that precede it in the enumeration. Note that after
		/// calling terms(), <see cref="TermEnum.Next()" /> must be called
		/// on the resulting enumeration before calling other methods such as
		/// <see cref="TermEnum.Term" />.
		/// </summary>
		/// <exception cref="System.IO.IOException">
        /// If there is a low-level IO error 
		/// </exception>
		public abstract TermEnum Terms(IState state);
		
		/// <summary>Returns an enumeration of all terms starting at a given term. If
		/// the given term does not exist, the enumeration is positioned at the
		/// first term greater than the supplied term. The enumeration is
		/// ordered by Term.compareTo(). Each term is greater than all that
		/// precede it in the enumeration.
        /// </summary>
        /// <exception cref="System.IO.IOException">
        /// If there is a low-level IO error
        /// </exception>
		public abstract TermEnum Terms(Term t, IState state);

        /// <summary>Returns the number of documents containing the term <c>t</c>.</summary>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public abstract int DocFreq(Term t, IState state);
		
		/// <summary>Returns an enumeration of all the documents which contain
		/// <c>term</c>. For each document, the document number, the frequency of
		/// the term in that document is also provided, for use in
		/// search scoring.  If term is null, then all non-deleted
		/// docs are returned with freq=1.
		/// Thus, this method implements the mapping:
		/// <p/><list>
		/// Term &#160;&#160; =&gt; &#160;&#160; &lt;docNum, freq&gt;<sup>*</sup>
		/// </list>
		/// <p/>The enumeration is ordered by document number.  Each document number
		/// is greater than all that precede it in the enumeration.
        /// </summary>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual TermDocs TermDocs(Term term, IState state)
		{
			EnsureOpen();
			TermDocs termDocs = TermDocs(state);
			termDocs.Seek(term, state);
			return termDocs;
		}

        /// <summary>Returns an unpositioned <see cref="Lucene.Net.Index.TermDocs" /> enumerator.</summary>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public abstract TermDocs TermDocs(IState state);
		
		/// <summary>Returns an enumeration of all the documents which contain
		/// <c>term</c>.  For each document, in addition to the document number
		/// and frequency of the term in that document, a list of all of the ordinal
		/// positions of the term in the document is available.  Thus, this method
		/// implements the mapping:
		/// 
		/// <p/><list>
		/// Term &#160;&#160; =&gt; &#160;&#160; &lt;docNum, freq,
		/// &lt;pos<sub>1</sub>, pos<sub>2</sub>, ...
		/// pos<sub>freq-1</sub>&gt;
		/// &gt;<sup>*</sup>
		/// </list>
		/// <p/> This positional information facilitates phrase and proximity searching.
		/// <p/>The enumeration is ordered by document number.  Each document number is
		/// greater than all that precede it in the enumeration.
        /// </summary>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual TermPositions TermPositions(Term term, IState state)
		{
			EnsureOpen();
			TermPositions termPositions = TermPositions(state);
			termPositions.Seek(term, state);
			return termPositions;
		}

        /// <summary>Returns an unpositioned <see cref="Lucene.Net.Index.TermPositions" /> enumerator.</summary>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public abstract TermPositions TermPositions(IState state);
		
		
		
		/// <summary>
		/// Deletes the document numbered <c>docNum</c>.  Once a document is
		/// deleted it will not appear in TermDocs or TermPostitions enumerations.
		/// Attempts to read its field with the <see cref="Documents.Document" />
		/// method will result in an error.  The presence of this document may still be
		/// reflected in the <see cref="DocFreq" /> statistic, though
		/// this will be corrected eventually as the index is further modified.
		/// </summary>
		/// <exception cref="StaleReaderException">
        /// If the index has changed since this reader was opened
		/// </exception>
		/// <exception cref="CorruptIndexException">If the index is corrupt</exception>
		/// <exception cref="LockObtainFailedException">
        /// If another writer has this index open (<c>write.lock</c> could not be obtained)
        /// </exception>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual void  DeleteDocument(int docNum, IState state)
		{
			lock (this)
			{
				EnsureOpen();
				AcquireWriteLock(state);
				hasChanges = true;
				DoDelete(docNum, state);
			}
		}
		
		
		/// <summary>Implements deletion of the document numbered <c>docNum</c>.
		/// Applications should call <see cref="DeleteDocument(int)" /> or <see cref="DeleteDocuments(Term)" />.
		/// </summary>
		protected internal abstract void  DoDelete(int docNum, IState state);
		
		
		/// <summary>
		/// Deletes all documents that have a given <c>term</c> indexed.
		/// This is useful if one uses a document field to hold a unique ID string for
		/// the document.  Then to delete such a document, one merely constructs a
		/// term with the appropriate field and the unique ID string as its text and
		/// passes it to this method.
		/// See <see cref="DeleteDocument(int)" /> for information about when this deletion will 
		/// become effective.
		/// </summary>
		/// <returns>The number of documents deleted</returns>
        /// <exception cref="StaleReaderException">
        /// If the index has changed since this reader was opened
        /// </exception>
        /// <exception cref="CorruptIndexException">If the index is corrupt</exception>
        /// <exception cref="LockObtainFailedException">
        /// If another writer has this index open (<c>write.lock</c> could not be obtained)
        /// </exception>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual int DeleteDocuments(Term term, IState state)
		{
			EnsureOpen();
			TermDocs docs = TermDocs(term, state);
			if (docs == null)
				return 0;
			int n = 0;
			try
			{
				while (docs.Next(state))
				{
					DeleteDocument(docs.Doc, state);
					n++;
				}
			}
			finally
			{
				docs.Close();
			}
			return n;
		}
		
		/// <summary>Undeletes all documents currently marked as deleted in this index.
		/// 
        /// </summary>
        /// <exception cref="StaleReaderException">
        /// If the index has changed since this reader was opened
        /// </exception>
        /// <exception cref="CorruptIndexException">If the index is corrupt</exception>
        /// <exception cref="LockObtainFailedException">
        /// If another writer has this index open (<c>write.lock</c> could not be obtained)
        /// </exception>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public virtual void  UndeleteAll(IState state)
		{
			lock (this)
			{
				EnsureOpen();
				AcquireWriteLock(state);
				hasChanges = true;
				DoUndeleteAll(state);
			}
		}
		
		/// <summary>Implements actual undeleteAll() in subclass. </summary>
		protected internal abstract void  DoUndeleteAll(IState state);
		
		/// <summary>
		/// Does nothing by default. Subclasses that require a write lock for
		/// index modifications must implement this method. 
		/// </summary>
		protected internal virtual void  AcquireWriteLock(IState state)
		{
			lock (this)
			{
				/* NOOP */
			}
		}
		
		/// <summary> </summary>
		/// <exception cref="System.IO.IOException" />
		public void  Flush(IState state)
		{
			lock (this)
			{
				EnsureOpen();
				Commit(state);
			}
		}
		
		/// <param name="commitUserData">Opaque Map (String -> String)
		/// that's recorded into the segments file in the index,
		/// and retrievable by <see cref="IndexReader.GetCommitUserData" />
        /// </param>
        /// <exception cref="System.IO.IOException" />
        public void Flush(IDictionary<string, string> commitUserData, IState state)
		{
			lock (this)
			{
				EnsureOpen();
				Commit(commitUserData, state);
			}
		}
		
		/// <summary> Commit changes resulting from delete, undeleteAll, or
		/// setNorm operations
		/// 
		/// If an exception is hit, then either no changes or all
		/// changes will have been committed to the index
		/// (transactional semantics).
		/// </summary>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
		public /*protected internal*/ void  Commit(IState state)
		{
			lock (this)
			{
				Commit(null, state);
			}
		}
		
		/// <summary> Commit changes resulting from delete, undeleteAll, or
		/// setNorm operations
		/// 
		/// If an exception is hit, then either no changes or all
		/// changes will have been committed to the index
		/// (transactional semantics).
		/// </summary>
		/// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
        public void Commit(IDictionary<string, string> commitUserData, IState state)
		{
			lock (this)
			{
				if (hasChanges)
				{
					DoCommit(commitUserData, state);
				}
				hasChanges = false;
			}
		}
		
		/// <summary>Implements commit.</summary>
	    protected internal abstract void DoCommit(IDictionary<string, string> commitUserData, IState state);

        [Obsolete("Use Dispose() instead")]
		public void Close()
		{
		    Dispose();
		}

        /// <summary> Closes files associated with this index.
        /// Also saves any new deletions to disk.
        /// No other methods should be called after this has been called.
        /// </summary>
        /// <exception cref="System.IO.IOException">If there is a low-level IO error</exception>
        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (this)
                {
                    if (!closed)
                    {
                        DecRef(StateHolder.Current.Value);
                        closed = true;
                    }
                }
            }
        }
		
		/// <summary>Implements close. </summary>
		protected internal abstract void  DoClose(IState state);
		
		
		/// <summary> Get a list of unique field names that exist in this index and have the specified
		/// field option information.
		/// </summary>
		/// <param name="fldOption">specifies which field option should be available for the returned fields
		/// </param>
		/// <returns> Collection of Strings indicating the names of the fields.
		/// </returns>
		/// <seealso cref="IndexReader.FieldOption">
		/// </seealso>
		public abstract ICollection<string> GetFieldNames(FieldOption fldOption);

	    /// <summary> Expert: return the IndexCommit that this reader has
	    /// opened.  This method is only implemented by those
	    /// readers that correspond to a Directory with its own
	    /// segments_N file.
	    /// 
	    /// <p/><b>WARNING</b>: this API is new and experimental and
	    /// may suddenly change.<p/>
	    /// </summary>
	    public virtual IndexCommit IndexCommit(IState state)
	    {
	        throw new NotSupportedException("This reader does not support this method.");
	    }

#if !NETSTANDARD2_1
        /// <summary> Prints the filename and size of each file within a given compound file.
        /// Add the -extract flag to extract files to the current working directory.
        /// In order to make the extracted version of the index work, you have to copy
        /// the segments file from the compound index into the directory where the extracted files are stored.
        /// </summary>
        /// <param name="args">Usage: Lucene.Net.Index.IndexReader [-extract] &lt;cfsfile&gt;
        /// </param>
        [STAThread]
		public static void  Main(String[] args)
		{
			System.String filename = null;
			bool extract = false;
			
			foreach (string t in args)
			{
				if (t.Equals("-extract"))
				{
					extract = true;
				}
				else if (filename == null)
				{
					filename = t;
				}
			}

	    	if (filename == null)
			{
				System.Console.Out.WriteLine("Usage: Lucene.Net.Index.IndexReader [-extract] <cfsfile>");
				return ;
			}
			
			Directory dir = null;
			CompoundFileReader cfr = null;
			
			try
			{
				var file = new System.IO.FileInfo(filename);
				System.String dirname = new System.IO.FileInfo(file.FullName).DirectoryName;
				filename = file.Name;
				dir = FSDirectory.Open(new System.IO.DirectoryInfo(dirname));
				cfr = new CompoundFileReader(dir, filename, null);
				
				System.String[] files = cfr.ListAll(null);
				System.Array.Sort(files); // sort the array of filename so that the output is more readable
				
				foreach (string t in files)
				{
					long len = cfr.FileLength(t, null);
					
					if (extract)
					{
						System.Console.Out.WriteLine("extract " + t + " with " + len + " bytes to local directory...");
						IndexInput ii = cfr.OpenInput(t, null);
						
						var f = new System.IO.FileStream(t, System.IO.FileMode.Create);
						
						// read and write with a small buffer, which is more effectiv than reading byte by byte
						var buffer = new byte[1024];
						int chunk = buffer.Length;
						while (len > 0)
						{
							var bufLen = (int) System.Math.Min(chunk, len);
							ii.ReadBytes(buffer, 0, bufLen, null);
							f.Write(buffer, 0, bufLen);
							len -= bufLen;
						}

                        f.Close();
                        ii.Close();
					}
					else
						System.Console.Out.WriteLine(t + ": " + len + " bytes");
				}
			}
			catch (System.IO.IOException ioe)
			{
				System.Console.Error.WriteLine(ioe.StackTrace);
			}
			finally
			{
				try
				{
					if (dir != null)
						dir.Close();
					if (cfr != null)
						cfr.Close();
				}
				catch (System.IO.IOException ioe)
				{
					System.Console.Error.WriteLine(ioe.StackTrace);
				}
			}
		}
#endif

        /// <summary>Returns all commit points that exist in the Directory.
        /// Normally, because the default is <see cref="KeepOnlyLastCommitDeletionPolicy" />
        ///, there would be only
        /// one commit point.  But if you're using a custom <see cref="IndexDeletionPolicy" />
        /// then there could be many commits.
        /// Once you have a given commit, you can open a reader on
        /// it by calling <see cref="IndexReader.Open(IndexCommit,bool)" />
        /// There must be at least one commit in
        /// the Directory, else this method throws <see cref="System.IO.IOException" />.  
        /// Note that if a commit is in
        /// progress while this method is running, that commit
        /// may or may not be returned array.  
        /// </summary>
        public static System.Collections.Generic.ICollection<IndexCommit> ListCommits(Directory dir, IState state)
		{
			return DirectoryReader.ListCommits(dir, state);
		}

	    /// <summary>Expert: returns the sequential sub readers that this
	    /// reader is logically composed of.  For example,
	    /// IndexSearcher uses this API to drive searching by one
	    /// sub reader at a time.  If this reader is not composed
	    /// of sequential child readers, it should return null.
	    /// If this method returns an empty array, that means this
	    /// reader is a null reader (for example a MultiReader
	    /// that has no sub readers).
	    /// <p/>
	    /// NOTE: You should not try using sub-readers returned by
	    /// this method to make any changes (setNorm, deleteDocument,
	    /// etc.). While this might succeed for one composite reader
	    /// (like MultiReader), it will most likely lead to index
	    /// corruption for other readers (like DirectoryReader obtained
	    /// through <see cref="IndexReader.Open(Lucene.Net.Store.Directory,bool)" />. Use the parent reader directly. 
	    /// </summary>
	    public virtual IndexReader[] GetSequentialSubReaders()
	    {
	        return null;
	    }

	    /// <summary>Expert</summary>
	    public virtual object FieldCacheKey
	    {
	        get { return this; }
	    }

	    /* Expert.  Warning: this returns null if the reader has
          *  no deletions 
          */

	    public virtual object DeletesCacheKey
	    {
	        get { return this; }
	    }

	    /// <summary>Returns the number of unique terms (across all fields)
	    /// in this reader.
	    /// 
	    /// This method returns long, even though internally
	    /// Lucene cannot handle more than 2^31 unique terms, for
	    /// a possible future when this limitation is removed.
	    /// 
	    /// </summary>
	    /// <throws>  UnsupportedOperationException if this count </throws>
	    /// <summary>  cannot be easily determined (eg Multi*Readers).
	    /// Instead, you should call <see cref="GetSequentialSubReaders" />
	    /// and ask each sub reader for
	    /// its unique term count. 
	    /// </summary>
	    public virtual long UniqueTermCount
	    {
	        get { throw new System.NotSupportedException("this reader does not implement getUniqueTermCount()"); }
	    }

	    /// <summary>
	    /// For IndexReader implementations that use
	    /// TermInfosReader to read terms, this returns the
	    /// current indexDivisor as specified when the reader was
	    /// opened.
	    /// </summary>
	    public virtual int TermInfosIndexDivisor
	    {
	        get { throw new NotSupportedException("This reader does not support this method."); }
	    }
	}
}